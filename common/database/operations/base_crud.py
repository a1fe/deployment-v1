"""
Базовый класс для CRUD операций
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session


class BaseCRUD:
    """Базовый класс для CRUD операций"""
    
    def __init__(self, model):
        """
        Инициализация с моделью
        
        Args:
            model: SQLAlchemy модель для CRUD операций
        """
        self.model = model
    
    def create(self, db: Session, obj_data: dict):
        """Создание нового объекта"""
        db_obj = self.model(**obj_data)
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj
    
    def get_by_id(self, db: Session, obj_id: Any):
        """Получение объекта по ID"""
        # Получаем имя primary key поля из модели
        primary_key = self._get_primary_key_column()
        return db.query(self.model).filter(getattr(self.model, primary_key) == obj_id).first()
    
    def get_all(self, db: Session, skip: int = 0, limit: int = 100) -> List:
        """Получение всех объектов с пагинацией"""
        return db.query(self.model).offset(skip).limit(limit).all()
    
    def update(self, db: Session, obj_id: Any, update_data: dict):
        """Обновление данных объекта"""
        primary_key = self._get_primary_key_column()
        db_obj = db.query(self.model).filter(getattr(self.model, primary_key) == obj_id).first()
        
        if db_obj:
            for key, value in update_data.items():
                setattr(db_obj, key, value)
            
            # Обновляем updated_at если поле существует
            if hasattr(db_obj, 'updated_at'):
                setattr(db_obj, 'updated_at', datetime.utcnow())
            
            db.commit()
            db.refresh(db_obj)
        
        return db_obj
    
    def delete(self, db: Session, obj_id: Any) -> bool:
        """Удаление объекта"""
        primary_key = self._get_primary_key_column()
        db_obj = db.query(self.model).filter(getattr(self.model, primary_key) == obj_id).first()
        
        if db_obj:
            db.delete(db_obj)
            db.commit()
            return True
        
        return False
    
    def _get_primary_key_column(self) -> str:
        """Получение имени поля primary key"""
        # Получаем primary key из SQLAlchemy модели
        try:
            primary_keys = [key.name for key in self.model.__table__.primary_key.columns]
            if primary_keys:
                return primary_keys[0]  # Берем первый primary key
        except AttributeError:
            pass
        
        # Fallback: пытаемся найти поле с _id в конце
        try:
            for column in self.model.__table__.columns:
                if column.name.endswith('_id'):
                    return column.name
        except AttributeError:
            pass
        
        raise ValueError(f"Не удается определить primary key для модели {self.model.__name__}")
