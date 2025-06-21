"""
Общие функции для инициализации базовых данных
"""

import sys
import os
sys.path.append('../..')
from database.operations.candidate_operations import (
    CompetencyCRUD, IndustryCRUD
)
from database.operations.company_operations import HiringStageCRUD
from utils.constants import (
    CORE_COMPETENCIES, 
    COMPANY_INDUSTRIES,
    DEFAULT_HIRING_STAGES
)


def create_base_competencies(db):
    """Создание базовых компетенций"""
    print("🛠️ Создание базовых компетенций...")
    created_competencies = []
    for comp_name in CORE_COMPETENCIES:
        comp = CompetencyCRUD.get_or_create(db, comp_name)
        created_competencies.append(comp)
    
    print(f"✅ Создано {len(created_competencies)} компетенций")
    return created_competencies


def create_base_industries(db):
    """Создание базовых отраслей"""
    print("🏭 Создание базовых отраслей...")
    created_industries = []
    for industry_name in COMPANY_INDUSTRIES:
        industry = IndustryCRUD.get_or_create(db, industry_name)
        created_industries.append(industry)
    
    print(f"✅ Создано {len(created_industries)} отраслей")
    return created_industries


def create_default_hiring_stages(db):
    """Создание стандартных этапов найма"""
    print("📋 Создание стандартных этапов найма...")
    
    created_stages = []
    for stage_info in DEFAULT_HIRING_STAGES:
        stage = HiringStageCRUD.get_or_create(db, stage_info['name'])
        created_stages.append(stage)
    
    print(f"✅ Создано {len(created_stages)} этапов найма")
    return created_stages
