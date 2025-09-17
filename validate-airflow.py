#!/usr/bin/env python3
"""
Validation script for Airflow 3.0 setup
Tests DAG syntax and imports
"""
import sys
import os
import importlib.util

def test_dag_imports():
    """Test that DAG files can be imported without errors"""
    print("=== Testing DAG Imports ===")
    
    dag_files = [
        'airflow/dags/assets.py',
        'airflow/dags/cdc_etl_orchestration.py', 
        'airflow/dags/etl_monitoring.py',
        'airflow/dags/airflow_etl_utils.py'
    ]
    
    # Add path for ETL modules simulation
    sys.path.append('python')
    
    passed = 0
    total = len(dag_files)
    
    for dag_file in dag_files:
        try:
            if os.path.exists(dag_file):
                spec = importlib.util.spec_from_file_location("test_module", dag_file)
                module = importlib.util.module_from_spec(spec)
                
                # Mock some dependencies that won't be available
                sys.modules['airflow.sdk.definitions.dag'] = type('MockModule', (), {
                    'dag': lambda *args, **kwargs: lambda func: func,
                    'DAG': object
                })()
                sys.modules['airflow.sdk.definitions.task'] = type('MockModule', (), {
                    'task': lambda *args, **kwargs: lambda func: func
                })()
                sys.modules['airflow.sdk.definitions.asset'] = type('MockModule', (), {
                    'Asset': lambda *args, **kwargs: object()
                })()
                
                spec.loader.exec_module(module)
                print(f"✓ {dag_file}: Import successful")
                passed += 1
            else:
                print(f"✗ {dag_file}: File not found")
        except Exception as e:
            print(f"✗ {dag_file}: Import failed - {str(e)}")
    
    return passed == total

def test_file_structure():
    """Test that required files and directories exist"""
    print("\n=== Testing File Structure ===")
    
    required_paths = [
        'airflow/',
        'airflow/dags/',
        'airflow/logs/',
        'airflow/plugins/',
        'airflow/config/',
        'airflow/dags/assets.py',
        'airflow/dags/cdc_etl_orchestration.py',
        'airflow/dags/etl_monitoring.py',
        'airflow/requirements.txt',
        'scripts/start-etl-airflow.sh',
        'scripts/monitor-etl-airflow.sh'
    ]
    
    passed = 0
    total = len(required_paths)
    
    for path in required_paths:
        if os.path.exists(path):
            print(f"✓ {path}: Exists")
            passed += 1
        else:
            print(f"✗ {path}: Missing")
    
    return passed == total

def test_docker_compose_airflow():
    """Test that docker-compose.yml includes Airflow services"""
    print("\n=== Testing Docker Compose Airflow Configuration ===")
    
    try:
        with open('docker-compose.yml', 'r') as f:
            content = f.read()
        
        required_services = [
            'airflow-db:',
            'airflow-init:',
            'airflow-webserver:',
            'airflow-scheduler:',
            'airflow-api:'
        ]
        
        passed = 0
        total = len(required_services)
        
        for service in required_services:
            if service in content:
                print(f"✓ {service}: Found in docker-compose.yml")
                passed += 1
            else:
                print(f"✗ {service}: Missing from docker-compose.yml")
        
        # Check for Airflow image
        if 'apache/airflow:3.0.0' in content:
            print("✓ Apache Airflow 3.0.0 image specified")
            passed += 1
            total += 1
        else:
            print("✗ Apache Airflow 3.0.0 image not found")
            total += 1
        
        return passed == total
        
    except Exception as e:
        print(f"✗ Error reading docker-compose.yml: {e}")
        return False

def test_environment_config():
    """Test that .env file includes Airflow configuration"""
    print("\n=== Testing Environment Configuration ===")
    
    try:
        with open('.env', 'r') as f:
            content = f.read()
        
        required_vars = [
            'AIRFLOW_UID=',
            'AIRFLOW__CORE__EXECUTOR=',
            'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=',
            '_AIRFLOW_WWW_USER_USERNAME=',
            '_AIRFLOW_WWW_USER_PASSWORD='
        ]
        
        passed = 0
        total = len(required_vars)
        
        for var in required_vars:
            if var in content:
                print(f"✓ {var}: Found in .env")
                passed += 1
            else:
                print(f"✗ {var}: Missing from .env")
        
        return passed == total
        
    except Exception as e:
        print(f"✗ Error reading .env file: {e}")
        return False

def main():
    """Run all validation tests"""
    print("=== Airflow 3.0 Setup Validation ===\n")
    
    tests = [
        ("File Structure", test_file_structure),
        ("Environment Configuration", test_environment_config),
        ("Docker Compose Configuration", test_docker_compose_airflow),
        ("DAG Imports", test_dag_imports),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        if test_func():
            passed += 1
            print(f"✓ {test_name}: PASSED")
        else:
            print(f"✗ {test_name}: FAILED")
    
    print(f"\n=== Results ===")
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("✓ All Airflow validation tests passed!")
        print("\nNext steps:")
        print("1. Run: ./scripts/start-etl-airflow.sh")
        print("2. Access Airflow UI: http://localhost:8080 (admin/admin)")
        print("3. Enable DAGs and monitor execution")
        return 0
    else:
        print("✗ Some Airflow tests failed!")
        print("Please check the failed tests and fix the issues before starting Airflow.")
        return 1

if __name__ == "__main__":
    sys.exit(main())