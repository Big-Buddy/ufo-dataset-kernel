from googleplaces import GooglePlaces, types, lang

YOUR_API_KEY = 'AIzaSyC6cc5yDR8w-RIVRMMaRCfc3dCFQ3ulL8w'

google_places = GooglePlaces(YOUR_API_KEY)
query_results = googleplaces.nearby_search(lat_lng={'lat' : 36.97417, 'lng' :-122.03})

print(query_results)
