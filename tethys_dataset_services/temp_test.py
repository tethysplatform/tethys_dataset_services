import os
from engines import GeoServerSpatialDatasetEngine

# Create Engine
engine = GeoServerSpatialDatasetEngine(endpoint='http://192.168.59.103:8181/geoserver/rest',
                                       username='admin',
                                       password='geoserver')

# UPDATE
# updated_resource = engine.update_resource(resource_id='roads',
#                                           enabled=True,
#                                           title='Even more spearfish roads',
#                                           debug=True)

# updated_layer = engine.update_layer(layer_id='roads',
#                                     default_style='simple_roads',
#                                     styles=['line'],
#                                     debug=True)

# updated_layer_group = engine.update_layer_group(layer_group_id='tasmania',
#                                                 layers=['tasmania_state_boundaries', 'tasmania_water_bodies',
#                                                         'tasmania_roads', 'tasmania_cities', 'roads'],
#                                                 styles=['green', 'cite_lakes', 'simple_roads', 'capitals',
#                                                         'simple_roads'],
#                                                 debug=True)



# DELETE
# engine.delete_layer_group(layer_group_id='my_layer_group', debug=True)
# engine.delete_layer(layer_id='my_layer', recurse=True, debug=True) # Check recurse (deletes layer group as well).
# engine.delete_layer(layer_id='my_layer', debug=True)
# engine.delete_resource(resource_id='Pk50095', recurse=True, debug=True)  # Belongs to layer
# engine.delete_resource(resource_id='bob', debug=True)  # Does not exist

# CREATE
# engine.get_layer_group('my_layer_group', debug=True)

# Layer Group
# layers = ('poi', 'tiger_roads')
# styles = ('line', 'line')
# bounds = ('-74.02722', '-73.907005', '40.684221', '40.878178', 'EPSG:4326')
# engine.create_layer_group('my_layer_group', layers=layers, styles=styles, bounds=bounds, debug=True)

# Shapefile Layer
# shapefile_base = "/Users/swainn/projects/tethysdev/tethys_dataset_services/tethys_dataset_services/tests/files/shapefile/bugsites"  # Test both base and zip archive methods
# # engine.create_shapefile_resource('cite:foo', shapefile_dir=shapefile_base, overwrite=True, debug=True)  # Handle overwrite in tests
# # shapefile_base = "/Users/swainn/projects/tethysdev/tethys_dataset_services/tethys_dataset_services/tests/files/shapefile/bar.zip"
# # engine.create_shapefile_resource('cite:bar', shapefile_base=shapefile_base, overwrite=True, debug=True)
# engine.create_shapefile_resource('bar', shapefile_base=shapefile_base, overwrite=True, debug=True)

# STORES AND WORKSPACES
# workspace = engine.get_workspace('sf', debug=True)
# garbage_workspace = engine.get_workspace('garbage', debug=True)
# store = engine.get_store('sf', debug=True)
# workspace_store = engine.get_store('sf:sfdem', debug=True)
# garbage_store = engine.get_workspace('garbage', debug=True)

# workspace = engine.create_workspace('fred', 'http://tethys.ci-water.org/fred', debug=True)
# workspace = engine.create_workspace('bob', 'http://tethys.ci-water.org/bob', debug=True)
# workspace = engine.create_workspace('bob', 'http://tethys.ci-water.org/bob', debug=True)  # Duplicate name fails
# workspace = engine.create_workspace('jan', 'http://tethys.ci-water.org/bob', debug=True)  # Duplicate URI fails

# engine.delete_workspace('fred', debug=True)
# engine.delete_workspace('bob', debug=True)
# engine.delete_workspace('bob', debug=True)

# engine.delete_store('c9x18bxx3m', debug=True)
# engine.delete_store('sf:tqnw8vce27', debug=True)
# engine.delete_store('garbage', debug=True)


# shapefile_base = "/Users/swainn/projects/tethysdev/tethys_dataset_services/tethys_dataset_services/tests/files/shapefile/test"  # Test both base and zip archive methods
# engine.create_shapefile_resource('sf:ldkeianeic', shapefile_base=shapefile_base, overwrite=True, debug=True)
#
# # resource = engine.get_resource('sf:sfdem', debug=True)
# resource = engine.get_resource('ldkeianeic', store='ldkeianeic', debug=True)
#
# response = engine.update_resource(resource_id='sf:ldkeianeic',
#                                   title='A new title.',
#                                   debug=True)

# response = engine.get_style(style_id='line', debug=True)  # Without workspace
# response = engine.get_style(style_id='sf:line', debug=True)  # With workspace NOTE: must specify workspace if belongs to workspace...

# ## CREATE STYLE
# sld = '/Users/swainn/projects/tethysdev/tethys_dataset_services/tethys_dataset_services/tests/files/point.sld'
# # with open(sld, 'r') as sld_file:
# #     response = engine.create_style(style_id='fred', sld=sld_file.read(), debug=True)
# #
# with open(sld, 'r') as sld_file:
#     response = engine.create_style(style_id='sf:fred', sld=sld_file.read(), debug=True)
#
# response = engine.delete_style(style_id='sf:fred', debug=True)

# response = engine.list_stores(debug=True)
# response = engine.list_stores(with_properties=True, debug=True)
# response = engine.list_stores(workspace='sf', with_properties=True, debug=True)

# response = engine.list_workspaces(debug=True)
# response = engine.list_workspaces(with_properties=True, debug=True)

# response = engine.list_styles(debug=True)
# response = engine.list_styles(with_properties=True, debug=True)

# response = engine.list_layers(with_properties=True, debug=True)
# response = engine.list_layer_groups(debug=True)
response = engine.list_resources(with_properties=True, debug=True)

# response = engine.get_layer(layer_id='sf:roads', debug=True)
# response = engine.get_layer(layer_id='sf:sfdem', debug=True)

response = engine.get_resource(resource_id='sf:roads', debug=True)
# response = engine.get_resource(resource_id='sf:sfdem', debug=True)



# SERVICES




