"""
Модель адреса кандидата
"""

from typing import Optional, Dict, Any
from sqlalchemy import Column, Integer, String, Text, ForeignKey, UUID
from sqlalchemy.orm import relationship
from ..base import Base


class Address(Base):
    """Модель адреса"""
    __tablename__ = 'addresses'
    
    address_id = Column(Integer, primary_key=True, autoincrement=True)
    submission_id = Column(UUID(as_uuid=True), ForeignKey('submissions.submission_id', ondelete='CASCADE'), nullable=False)
    address = Column(Text, nullable=False)
    city = Column(String(100), nullable=False)
    state_province = Column(String(100), nullable=False)
    zip_postal_code = Column(String(20), nullable=False)
    country = Column(String(100), nullable=False)
    
    # Отношения
    submission = relationship("Submission", back_populates="addresses")
    
    def __repr__(self) -> str:
        city_val = getattr(self, 'city', 'Unknown')
        country_val = getattr(self, 'country', 'Unknown')
        return f"<Address(id={self.address_id}, city='{city_val}', country='{country_val}')>"
    
    def get_full_address(self) -> str:
        """Полный адрес в одной строке"""
        address_val = getattr(self, 'address', '') or ''
        city_val = getattr(self, 'city', '') or ''
        state_val = getattr(self, 'state_province', '') or ''
        zip_val = getattr(self, 'zip_postal_code', '') or ''
        country_val = getattr(self, 'country', '') or ''
        
        parts = [address_val, city_val, state_val, zip_val, country_val]
        return ', '.join(part for part in parts if part)
    
    def get_city_state(self) -> str:
        """Город и штат/область"""
        city_val = getattr(self, 'city', '') or ''
        state_val = getattr(self, 'state_province', '') or ''
        
        if city_val and state_val:
            return f"{city_val}, {state_val}"
        elif city_val:
            return city_val
        elif state_val:
            return state_val
        else:
            return "Не указано"
    
    def is_complete(self) -> bool:
        """Проверка полноты адреса"""
        required_fields = ['address', 'city', 'state_province', 'zip_postal_code', 'country']
        for field in required_fields:
            value = getattr(self, field, None)
            if not value or not str(value).strip():
                return False
        return True
