"""借读数暂估与故障时段撤回路由。"""

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field, model_validator

from . import provisional_service as service
from .auth import current_user, require

router = APIRouter(prefix='/provisional', tags=['暂估借数'])


class EstimateIn(BaseModel):
    wave_id: int
    source_wave_id: int
    window_size: int = Field(default=8, ge=4, le=24)


class WithdrawalIn(BaseModel):
    wave_id: int
    belt_code: str | None = Field(default=None, max_length=60)
    window_start: datetime
    window_end: datetime
    reason: str = Field(min_length=2, max_length=300)

    @model_validator(mode='after')
    def _check_window(self):
        if self.window_end <= self.window_start:
            raise ValueError('故障结束时间必须晚于开始时间')
        return self


@router.get('/lendable/{wave_id}')
def lendable(wave_id: int, user=Depends(current_user)):
    """目标波次可借的同航站楼已结束供数波次与剩余有效读数。"""
    return {'wave_id': wave_id, 'min_points': service.MIN_POINTS,
            'sources': service.lendable_waves(wave_id)}


@router.post('/estimates', status_code=201)
def create_estimate(data: EstimateIn, user=Depends(require('operator', 'supervisor'))):
    """借供数波次读数生成暂估，仅供值班参考，不可用于发布排班。"""
    return service.create_estimate(
        data.wave_id, data.source_wave_id, data.window_size, user
    )


@router.get('/estimates')
def estimates(wave_id: int | None = Query(default=None), user=Depends(current_user)):
    """暂估清单：供数波次、用数时段、读数明细与失效原因。"""
    return service.list_estimates(wave_id)


@router.post('/withdrawals', status_code=201)
def create_withdrawal(data: WithdrawalIn, user=Depends(require('supervisor'))):
    """主管撤回供数波次故障时段；借过该时段读数的有效暂估立即失效。"""
    withdrawal, invalidated = service.withdraw_window(
        data.wave_id, data.belt_code, data.window_start, data.window_end,
        data.reason.strip(), user,
    )
    return {'withdrawal': withdrawal, 'invalidated_estimate_ids': invalidated}


@router.get('/withdrawals')
def withdrawals(wave_id: int | None = Query(default=None), user=Depends(current_user)):
    """故障撤回记录与受影响的暂估数量。"""
    return service.list_withdrawals(wave_id)
