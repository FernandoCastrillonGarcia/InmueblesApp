from database import QdrantSingleton
from utils import points_that_dont_work

points = points_that_dont_work('Arriendo', local = True)

for point in points():
    pass