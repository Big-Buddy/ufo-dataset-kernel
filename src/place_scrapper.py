from googleplaces import GooglePlaces, types, lang

YOUR_API_KEY = 'AIzaSyC6cc5yDR8w-RIVRMMaRCfc3dCFQ3ulL8w'

google_places = GooglePlaces(YOUR_API_KEY)
query_result = googleplaces.nearby_search(lat_lng={'lat' : , 'lng' :})