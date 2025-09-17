#!/usr/bin/env python3
"""
Validation script to test ETL system components
"""
import sys
import os

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Test that all modules can be imported"""
    try:
        import config
        print("✓ Config module imported successfully")
        
        import utils
        print("✓ Utils module imported successfully")
        
        # Test configuration loading
        db_config, kafka_config, redis_config, app_config = config.get_config()
        print("✓ Configuration loaded successfully")
        print(f"  - Database: {db_config.host}:{db_config.port}")
        print(f"  - Kafka: {kafka_config.broker}")
        print(f"  - Redis: {redis_config.host}:{redis_config.port}")
        
        return True
    except Exception as e:
        print(f"✗ Import test failed: {e}")
        return False

def test_data_structures():
    """Test basic data structure operations"""
    try:
        from faker import Faker
        fake = Faker()
        
        # Test fake data generation
        name = fake.name()
        email = fake.email()
        print(f"✓ Faker library working: {name} ({email})")
        
        return True
    except Exception as e:
        print(f"✗ Data structure test failed: {e}")
        return False

def main():
    """Run all validation tests"""
    print("=== ETL System Validation ===")
    
    tests = [
        ("Module Imports", test_imports),
        ("Data Structures", test_data_structures),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        if test_func():
            passed += 1
        else:
            print(f"Failed: {test_name}")
    
    print(f"\n=== Results ===")
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("✓ All validation tests passed!")
        return 0
    else:
        print("✗ Some tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())