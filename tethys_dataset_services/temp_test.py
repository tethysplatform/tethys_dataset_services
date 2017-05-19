import os
from .engines import GeoServerSpatialDatasetEngine, CkanDatasetEngine

# Create Engine
# engine = GeoServerSpatialDatasetEngine(endpoint='http://ciwmap.chpc.utah.edu/geoserver/rest',
#                                        username='admin',
#                                        password='geoserver')

# engine = GeoServerSpatialDatasetEngine(endpoint='http://192.168.59.103:8181/geoserver/rest',
#                                        username='admin',
#                                        password='geoserver')

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
# shapefile_base = "/Users/swainn/Downloads/NHD_Flowlines/NHDFlowLine_12"
# engine.create_shapefile_resource('erfp:nhd_flowlines_12', shapefile_base=shapefile_base, overwrite=True, debug=True)
# engine.get_resource('cite:nhd_flowline', debug=True)

# shapefile_base = "/Users/swainn/Downloads/NHD_Catchments/NHDcatchments_12"
# engine.create_shapefile_resource('nhd_catchments', shapefile_base=shapefile_base, overwrite=True, debug=True)

# resource = engine.get_layer('topp:states', debug=True)
#resource = engine.get_resource('ldkeianeic', store='ldkeianeic', debug=True)
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
# response = engine.list_layers(debug=True)
# response = engine.list_layer_groups(debug=True)
# response = engine.list_resources(with_properties=True, debug=True)

# response = engine.get_layer(layer_id='sf:nhd_flowline', debug=True)
# response = engine.get_resource(resource_id='sf:nhd_catchments', debug=True)

# response = engine.get_resource(resource_id='sf:roads', debug=True)
# response = engine.get_resource(resource_id='sf:sfdem', debug=True)

# response = engine.get_layer(layer_id='sf:roads', debug=True)
# response = engine.get_layer(layer_id='sf:sfdem', debug=True)

# response = engine.get_layer_group('tiger-ny', debug=True)

# coverage_file = '/Users/swainn/testing/geoserver/upload/arc_sample/precip30min.zip'
# response = engine.create_coverage_resource(resource_id='my_arc_coverage', coverage_file=coverage_file,
#                                            coverage_type='arcgrid', overwrite=True, debug=True)

# coverage_file = '/Users/swainn/testing/geoserver/upload/sfdem.tif'
# response = engine.create_coverage_resource(store_id='my_geotiff', coverage_file=coverage_file,
#                                            coverage_type='geotiff', overwrite=True, debug=True)

# coverage_file = '/Users/swainn/testing/geoserver/upload/mosaic_sample/global_mosaic.zip'
# response = engine.create_coverage_resource(store_id='my_mosaic', coverage_file=coverage_file,
#                                            coverage_type='imagemosaic', overwrite=True, debug=True)

# coverage_file = '/Users/swainn/testing/geoserver/upload/img_sample/usa.zip'
# response = engine.create_coverage_resource(store_id='my_usa', coverage_file=coverage_file,
#                                            coverage_type='worldimage', overwrite=True, debug=True)

# coverage_file = '/Users/swainn/testing/geoserver/upload/img_sample/Pk50095.zip'
# response = engine.create_coverage_resource(store_id='my_pk', coverage_file=coverage_file,
#                                            coverage_type='worldimage', overwrite=True, debug=True)

# coverage_file = '/Users/swainn/testing/geoserver/upload/img_sample/foo.zip'
# response = engine.create_coverage_resource(store_id='nurc:my_foo', coverage_file=coverage_file,
#                                            coverage_type='worldimage', overwrite=True, debug=True)

# coverage_file = '/Users/swainn/testing/geoserver/upload/img_sample/usa.zip'
# response = engine.create_coverage_resource(store_id='topp:my_foo', coverage_file=coverage_file,
#                                            coverage_type='worldimage', overwrite=True, debug=True)

# engine.list_resources(store='my_foo', with_properties=True, debug=True)

# coverage_file = '/Users/swainn/testing/geoserver/upload/grass_ascii/my_grass.zip'
# response = engine.create_coverage_resource(store_id='topp:my_grass', coverage_file=coverage_file,
#                                            coverage_type='grassgrid', overwrite=True, debug=True)

# coverage_file = '/Users/swainn/testing/geoserver/upload/geotiff_sample/sfdem_no_prj.zip'
# response = engine.create_coverage_resource(store_id='testing:a_geotiff', coverage_file=coverage_file,
#                                            coverage_type='geotiff', overwrite=True, debug=True)

# shapefile_base = "/Users/swainn/projects/tethysdev/tethys_dataset_services/tethys_dataset_services/tests/files/shapefile/test"  # Test both base and zip archive methods
# engine.create_shapefile_resource('my_foo_bar', shapefile_base=shapefile_base, overwrite=True, debug=True)


# Two tables from the same database
# response = engine.create_postgis_resource(store_id='sf:death_star_db', table='states', host='192.168.59.103', port='5435',
#                                           database='death_star_example_db', user='tethys_super', password='pass',
#                                           debug=True)
#
# response = engine.create_postgis_resource(store_id='sf:death_star_db', table='darth_states', host='192.168.59.103', port='5435',
#                                           database='death_star_example_db', user='tethys_super', password='pass',
#                                           debug=True)

# Create store first then add table
# response = engine.create_postgis_resource(store_id='sf:another_death_star_db', host='192.168.59.103', port='5435',
#                                           database='death_star_example_db', user='tethys_super', password='pass',
#                                           debug=True)

# response = engine.add_table_to_postgis_store(store_id='sf:another_death_star_db', table='states')
# response = engine.add_table_to_postgis_store(store_id='sf:another_death_star_db', table='darth_states')

# shapefile_base = "/Users/swainn/Desktop/Counties/Counties"
# shapefile_zip = "/Users/swainn/Desktop/UtahMajorRiversPoly/Archive.zip"
# engine.create_shapefile_resource('utah:counties', shapefile_base=shapefile_base, overwrite=True, debug=True)
# engine.create_shapefile_resource('utah:rivers', shapefile_zip=shapefile_zip, overwrite=True, debug=True)


### ALAN's DATA ###
# shapefile_base = "/Users/swainn/Downloads/NHD_Flowlines 2/NHDFlowLine_12"
# engine.create_shapefile_resource('erfp:nhd_flowlines_12', shapefile_base=shapefile_base, overwrite=True, debug=True)

# shapefile_base = "/Users/swainn/Downloads/NHD_region_12_gage/region_12_gages"
# engine.create_shapefile_resource('sf:new2', shapefile_base=shapefile_base, overwrite=False, debug=True)

# # Valid
# engine = CkanDatasetEngine(endpoint='http://ciwckan.chpc.utah.edu/api/3/action',
#                            apikey='')
# engine.validate()
#
# # Invalid URL
# engine = CkanDatasetEngine(endpoint='htdu/api/3/action',
#                            apikey='')
# engine.validate()
#
# # Invalid Endpoint
# engine = CkanDatasetEngine(endpoint='http://example.com/api/3/action',
#                            apikey='')
# engine.validate()

# engine = GeoServerSpatialDatasetEngine(endpoint='http://192.168.59.103:8181/geoserver/rest',
#                                       username='admin',
#                                       password='geoserver')

# engine.validate()
