from zenml import pipeline
import pipelines.scrapping.steps as steps

@pipeline
def scraping_pipeline():
    """Pipeline to scrape property data"""
    
    operations = ['Arriendo', 'Venta']
    property_types = [
        'Casa',
        'Apartamento',
        'Lote',
        'Local',
        'Oficina',
        'Finca',
        'Parqueadero',
        'Consultorio',
        'Edificio',
        'Apartaestudio',
        'Cabaña',
        'Casa Campestre',
        'Casa Lote',
        'Habitación',
        'Bodega'
    ]
    # Step 1 Scrape Finca Raiz properties
    stats = steps.scrape_properties(operations, property_types)

    # Step 2 Create the signature
    steps.validate_scrapping_signature(stats)
