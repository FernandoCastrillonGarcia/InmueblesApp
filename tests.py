from pipelines.scrapping import scrape_properties_op

# 1. Create a dummy class that mimics a Kubeflow Artifact
class MockDataset:
    def __init__(self, path):
        self.path = path

# 2. Tell the function to save the CSV locally to your current folder
dummy_out = MockDataset("./local_test_stats.csv")

operations = ['Arriendo', 'Venta']
property_types = ['Casa']  # Just doing one for a fast test

# 3. Pass the dummy object to the function
scrape_properties_op.python_func(
    operations=operations, 
    properties=property_types,
    stats_out=dummy_out
)

print("Scraping complete! Check your folder for local_test_stats.csv")
