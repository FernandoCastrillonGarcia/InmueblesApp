import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipelines.scrapping import scrape_properties_op, validate_scrapping_signature_op

# Mock class to simulate Kubeflow Artifacts locally
class MockDataset:
    def __init__(self, path):
        self.path = path

if __name__ == "__main__":
    print("🧪 Testing Scraping Component...")
    dummy_out = MockDataset("./local_test_stats.csv")
    
    # We restrict to 1 operation and 1 property type for a fast local test
    operations = ['Arriendo']
    property_types = ['Casa']
    
    scrape_properties_op.python_func(
        operations=operations, 
        properties=property_types,
        stats_out=dummy_out
    )
    print("✅ Scraping component finished. Stats saved to local_test_stats.csv.")

    print("\n🧪 Testing Validation Component...")
    dummy_comparison = MockDataset("./local_test_comparison.csv")
    
    validate_scrapping_signature_op.python_func(
        current_stats_in=dummy_out,
        comparison_out=dummy_comparison
    )
    print("✅ Validation component finished. Comparison saved to local_test_comparison.csv.")
