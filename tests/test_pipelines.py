import sys
import os

# Let this test file import from the dags/ folder
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

from weather_pipeline import clean_weather_record
from healthcare_pipeline import (
    clean_hospital_record,
    is_valid_record,
    clean_readmission_record,
    is_valid_readmission_record,
)


# --- Tests for weather_pipeline.py ---

def test_clean_weather_record_extracts_correct_fields():
    raw_data = {
        'current_weather': {
            'temperature': 22.5,
            'windspeed': 10.2,
            'time': '2026-09-21T12:00',
        }
    }
    result = clean_weather_record(raw_data)

    assert result['temperature_c'] == 22.5
    assert result['windspeed_kmh'] == 10.2
    assert result['observed_at'] == '2026-09-21T12:00'


# --- Tests for healthcare_pipeline.py ---

def test_clean_hospital_record_maps_fields_correctly():
    raw_record = {
        'facility_id': '010001',
        'facility_name': 'TEST HOSPITAL',
        'state': 'IL',
        'hospital_ownership': 'Voluntary non-profit',
        'hospital_overall_rating': '4',
    }
    result = clean_hospital_record(raw_record)

    assert result['facility_id'] == '010001'
    assert result['facility_name'] == 'TEST HOSPITAL'
    assert result['state'] == 'IL'
    assert result['ownership_type'] == 'Voluntary non-profit'
    assert result['overall_rating'] == '4'


def test_clean_hospital_record_handles_missing_fields():
    raw_record = {'facility_name': 'INCOMPLETE HOSPITAL'}
    result = clean_hospital_record(raw_record)

    assert result['facility_id'] is None
    assert result['facility_name'] == 'INCOMPLETE HOSPITAL'
    assert result['state'] is None
    assert result['overall_rating'] is None


def test_is_valid_record_accepts_good_record():
    record = {'facility_id': '010001', 'state': 'IL', 'overall_rating': '4'}
    assert is_valid_record(record) is True


def test_is_valid_record_rejects_missing_state():
    record = {'facility_id': '010001', 'state': None, 'overall_rating': '4'}
    assert is_valid_record(record) is False


def test_is_valid_record_rejects_not_available_rating():
    record = {'facility_id': '010001', 'state': 'IL', 'overall_rating': 'Not Available'}
    assert is_valid_record(record) is False


def test_is_valid_record_rejects_none_rating():
    record = {'facility_id': '010001', 'state': 'IL', 'overall_rating': None}
    assert is_valid_record(record) is False


def test_is_valid_record_rejects_missing_facility_id():
    record = {'facility_id': None, 'state': 'IL', 'overall_rating': '4'}
    assert is_valid_record(record) is False

# --- Tests for readmissions data ---

def test_clean_readmission_record_maps_fields_correctly():
    raw_record = {
        'facility_id': '010001',
        'measure_name': 'READM-30-HIP-KNEE-HRRP',
        'excess_readmission_ratio': '0.9875',
        'predicted_readmission_rate': '4.5734',
        'expected_readmission_rate': '4.6311',
        'start_date': '07/01/2021',
        'end_date': '06/30/2024',
    }
    result = clean_readmission_record(raw_record)

    assert result['facility_id'] == '010001'
    assert result['measure_name'] == 'READM-30-HIP-KNEE-HRRP'
    assert result['excess_readmission_ratio'] == '0.9875'


def test_is_valid_readmission_record_accepts_good_record():
    record = {
        'facility_id': '010001',
        'measure_name': 'READM-30-HIP-KNEE-HRRP',
        'excess_readmission_ratio': '0.9875',
    }
    assert is_valid_readmission_record(record) is True


def test_is_valid_readmission_record_rejects_missing_facility_id():
    record = {
        'facility_id': None,
        'measure_name': 'READM-30-HIP-KNEE-HRRP',
        'excess_readmission_ratio': '0.9875',
    }
    assert is_valid_readmission_record(record) is False


def test_is_valid_readmission_record_rejects_missing_measure_name():
    record = {
        'facility_id': '010001',
        'measure_name': None,
        'excess_readmission_ratio': '0.9875',
    }
    assert is_valid_readmission_record(record) is False


def test_is_valid_readmission_record_rejects_non_numeric_ratio():
    record = {
        'facility_id': '010001',
        'measure_name': 'READM-30-HIP-KNEE-HRRP',
        'excess_readmission_ratio': 'Too Few to Report',
    }
    assert is_valid_readmission_record(record) is False


def test_is_valid_readmission_record_rejects_na_ratio():
    record = {
        'facility_id': '010001',
        'measure_name': 'READM-30-HIP-KNEE-HRRP',
        'excess_readmission_ratio': 'N/A',
    }
    assert is_valid_readmission_record(record) is False