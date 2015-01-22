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
# engine.delete_layer_group(layer_group_id='mine', debug=True)
engine.delete_layer(layer_id='poly_landmarks_mine', debug=True)


# CREATE



# SERVICES




