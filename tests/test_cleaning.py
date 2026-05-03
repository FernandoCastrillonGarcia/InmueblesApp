from pipelines.cleaning import (
    remove_duplicates_by_source_op,
    remove_erroneous_values_op,
    cap_prices_by_property_type_op,
    cap_numeric_fields_op
)

if __name__ == "__main__":
    print("🧪 Testing Cleaning Pipeline Components...")
    
    USE_LOCAL_DB = True
    collections = ["Arriendo", "Venta"]
    
    print("\n1. Removing duplicates...")
    dedup_stats = remove_duplicates_by_source_op.python_func(local=USE_LOCAL_DB)
    print(f"Stats: {dedup_stats}")
    
    print("\n2. Removing erroneous values...")
    err_stats = remove_erroneous_values_op.python_func(collections=collections, local=USE_LOCAL_DB)
    print(f"Stats: {err_stats}")
    
    print("\n3. Capping prices by property type...")
    price_stats = cap_prices_by_property_type_op.python_func(collections=collections, local=USE_LOCAL_DB)
    print(f"Stats: {price_stats}")
    
    print("\n4. Capping numeric fields...")
    num_stats = cap_numeric_fields_op.python_func(collections=collections, local=USE_LOCAL_DB)
    print(f"Stats: {num_stats}")
    
    print("\n✅ All Cleaning components completed locally!")
