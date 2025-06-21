"""
Модуль для предобработки текста перед созданием эмбеддингов
"""

import re
import unicodedata
from typing import Optional, Dict, Any


class TextPreprocessor:
    """Класс для предобработки текста перед созданием эмбеддингов"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Инициализация препроцессора
        
        Args:
            config: Конфигурация препроцессора
                - remove_extra_whitespace: Удалять лишние пробелы (по умолчанию True)
                - normalize_line_breaks: Нормализовать переносы строк (по умолчанию True)
                - remove_duplicates: Удалять дублирующиеся предложения (по умолчанию True)
                - min_sentence_length: Минимальная длина предложения (по умолчанию 10)
                - normalize_unicode: Нормализовать Unicode символы (по умолчанию True)
                - preserve_structure: Сохранять структуру документа (по умолчанию True)
        """
        default_config = {
            'remove_extra_whitespace': True,
            'normalize_line_breaks': True,
            'remove_duplicates': True,
            'min_sentence_length': 10,
            'normalize_unicode': True,
            'preserve_structure': True,
            'remove_empty_lines': True,
            'max_consecutive_newlines': 2
        }
        
        self.config = {**default_config, **(config or {})}
    
    def preprocess(self, text: str) -> str:
        """
        Основная функция предобработки текста
        
        Args:
            text: Исходный текст
            
        Returns:
            Обработанный текст
        """
        if not text or not isinstance(text, str):
            return ""
        
        # 1. Нормализация Unicode
        if self.config['normalize_unicode']:
            text = self._normalize_unicode(text)
        
        # 2. Удаление лишних пробелов и табуляций
        if self.config['remove_extra_whitespace']:
            text = self._remove_extra_whitespace(text)
        
        # 3. Нормализация переносов строк
        if self.config['normalize_line_breaks']:
            text = self._normalize_line_breaks(text)
        
        # 4. Удаление пустых строк
        if self.config['remove_empty_lines']:
            text = self._remove_empty_lines(text)
        
        # 5. Ограничение количества подряд идущих переносов строк
        text = self._limit_consecutive_newlines(text)
        
        # 6. Удаление дублирующихся предложений
        if self.config['remove_duplicates']:
            text = self._remove_duplicate_sentences(text)
        
        # 7. Финальная очистка
        text = self._final_cleanup(text)
        
        return text.strip()
    
    def _normalize_unicode(self, text: str) -> str:
        """Нормализация Unicode символов"""
        # Нормализация в NFC форму (каноническая композиция)
        text = unicodedata.normalize('NFC', text)
        
        # Замена специальных пробельных символов на обычные пробелы
        text = re.sub(r'[\u00A0\u1680\u180E\u2000-\u200B\u202F\u205F\u3000\uFEFF]', ' ', text)
        
        # Замена различных видов тире на стандартный дефис
        text = re.sub(r'[–—―]', '-', text)
        
        # Замена различных видов кавычек на стандартные
        text = re.sub(r'[""„‚''‛]', '"', text)
        
        return text
    
    def _remove_extra_whitespace(self, text: str) -> str:
        """Удаление лишних пробелов и табуляций"""
        # Замена всех пробельных символов (кроме переносов строк) на обычные пробелы
        text = re.sub(r'[^\S\n]+', ' ', text)
        
        # Удаление пробелов в начале и конце строк
        lines = text.split('\n')
        lines = [line.strip() for line in lines]
        text = '\n'.join(lines)
        
        return text
    
    def _normalize_line_breaks(self, text: str) -> str:
        """Нормализация переносов строк"""
        # Замена всех видов переносов строк на стандартный \n
        text = re.sub(r'\r\n|\r', '\n', text)
        
        # Удаление переносов строк в середине предложений (где нет знаков препинания)
        if self.config['preserve_structure']:
            # Более консервативный подход - объединяем строки только если:
            # - предыдущая строка не заканчивается знаком препинания
            # - следующая строка не начинается с заглавной буквы или специальных символов
            text = re.sub(r'(?<=[a-zа-я])\n(?=[a-zа-я])', ' ', text)
        else:
            # Агрессивный подход - объединяем все строки без явных разделителей
            text = re.sub(r'(?<=[^\.\!\?\:\;\n])\n(?=[^\n\-\*\d\s])', ' ', text)
        
        return text
    
    def _remove_empty_lines(self, text: str) -> str:
        """Удаление пустых строк"""
        lines = text.split('\n')
        non_empty_lines = [line for line in lines if line.strip()]
        return '\n'.join(non_empty_lines)
    
    def _limit_consecutive_newlines(self, text: str) -> str:
        """Ограничение количества подряд идущих переносов строк"""
        max_newlines = self.config['max_consecutive_newlines']
        pattern = r'\n{' + str(max_newlines + 1) + ',}'
        replacement = '\n' * max_newlines
        return re.sub(pattern, replacement, text)
    
    def _remove_duplicate_sentences(self, text: str) -> str:
        """Удаление дублирующихся предложений"""
        # Разбиваем текст на предложения по разным разделителям
        sentences = re.split(r'[.!?]+|\n+', text)
        
        # Очищаем предложения и удаляем дубликаты
        seen_sentences = set()
        unique_sentences = []
        
        for sentence in sentences:
            # Очищаем предложение
            clean_sentence = re.sub(r'\s+', ' ', sentence.strip()).lower()
            
            # Пропускаем слишком короткие предложения
            if len(clean_sentence) < self.config['min_sentence_length']:
                continue
            
            # Добавляем только уникальные предложения
            if clean_sentence not in seen_sentences:
                seen_sentences.add(clean_sentence)
                # Сохраняем оригинальное предложение (с правильным регистром)
                original_sentence = sentence.strip()
                if original_sentence:
                    unique_sentences.append(original_sentence)
        
        # Собираем текст обратно
        if unique_sentences:
            # Соединяем предложения, сохраняя структуру
            result = []
            for sentence in unique_sentences:
                if sentence.endswith(('.', '!', '?')):
                    result.append(sentence)
                elif any(sentence.startswith(prefix) for prefix in ['•', '-', '*', '1.', '2.', '3.', '4.', '5.']):
                    # Для списков добавляем перенос строки
                    result.append(sentence)
                else:
                    # Для обычных предложений добавляем точку
                    result.append(sentence + '.')
            
            # Соединяем с учетом структуры
            text_result = []
            for i, sentence in enumerate(result):
                if sentence.startswith(('•', '-', '*')) or sentence[0].isdigit():
                    # Элементы списка - добавляем перенос строки
                    text_result.append('\n' + sentence if text_result else sentence)
                else:
                    # Обычные предложения - добавляем пробел или перенос
                    if text_result and not text_result[-1].endswith('\n'):
                        text_result.append(' ' + sentence)
                    else:
                        text_result.append(sentence)
            
            return ''.join(text_result)
        
        return text
    
    def _final_cleanup(self, text: str) -> str:
        """Финальная очистка текста"""
        # Удаление множественных пробелов
        text = re.sub(r' {2,}', ' ', text)
        
        # Удаление пробелов перед знаками препинания
        text = re.sub(r' +([,.!?;:])', r'\1', text)
        
        # Добавление пробела после знаков препинания если его нет
        text = re.sub(r'([,.!?;:])([^\s\n])', r'\1 \2', text)
        
        # Удаление пробелов в начале и конце
        text = text.strip()
        
        return text
    
    def get_preprocessing_stats(self, original_text: str, processed_text: str) -> Dict[str, Any]:
        """
        Получение статистики предобработки
        
        Args:
            original_text: Исходный текст
            processed_text: Обработанный текст
            
        Returns:
            Словарь со статистикой
        """
        return {
            'original_length': len(original_text),
            'processed_length': len(processed_text),
            'compression_ratio': len(processed_text) / len(original_text) if original_text else 0,
            'original_lines': original_text.count('\n') + 1 if original_text else 0,
            'processed_lines': processed_text.count('\n') + 1 if processed_text else 0,
            'removed_characters': len(original_text) - len(processed_text),
        }


# Готовые конфигурации для разных типов документов

# Конфигурация для резюме
RESUME_PREPROCESSING_CONFIG = {
    'remove_extra_whitespace': True,
    'normalize_line_breaks': True,
    'remove_duplicates': True,
    'min_sentence_length': 8,  # Короче для резюме
    'normalize_unicode': True,
    'preserve_structure': True,  # Важно сохранить структуру резюме
    'remove_empty_lines': True,
    'max_consecutive_newlines': 2
}

# Конфигурация для описаний вакансий
JOB_DESCRIPTION_PREPROCESSING_CONFIG = {
    'remove_extra_whitespace': True,
    'normalize_line_breaks': True,
    'remove_duplicates': True,
    'min_sentence_length': 10,
    'normalize_unicode': True,
    'preserve_structure': True,  # Сохраняем структуру требований
    'remove_empty_lines': True,
    'max_consecutive_newlines': 2
}

# Агрессивная конфигурация для сильно зашумленных текстов
AGGRESSIVE_PREPROCESSING_CONFIG = {
    'remove_extra_whitespace': True,
    'normalize_line_breaks': True,
    'remove_duplicates': True,
    'min_sentence_length': 15,
    'normalize_unicode': True,
    'preserve_structure': False,  # Не сохраняем структуру
    'remove_empty_lines': True,
    'max_consecutive_newlines': 1
}


def preprocess_resume_text(text: str) -> str:
    """Предобработка текста резюме"""
    preprocessor = TextPreprocessor(RESUME_PREPROCESSING_CONFIG)
    return preprocessor.preprocess(text)


def preprocess_job_description_text(text: str) -> str:
    """Предобработка текста описания вакансии"""
    preprocessor = TextPreprocessor(JOB_DESCRIPTION_PREPROCESSING_CONFIG)
    return preprocessor.preprocess(text)


def preprocess_text_with_stats(text: str, config: Optional[Dict[str, Any]] = None) -> tuple[str, Dict[str, Any]]:
    """
    Предобработка текста с возвратом статистики
    
    Returns:
        Tuple из (обработанный_текст, статистика)
    """
    preprocessor = TextPreprocessor(config)
    processed_text = preprocessor.preprocess(text)
    stats = preprocessor.get_preprocessing_stats(text, processed_text)
    return processed_text, stats
