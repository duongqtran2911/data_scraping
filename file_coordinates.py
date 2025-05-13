from opencage.geocoder import OpenCageGeocode

key = '38bc4c2dab6548beaa38404b4318d24b'
geocoder = OpenCageGeocode(key)

query = u'19 Nguyễn Đình Chiểu, Đa Kao, Quận 1, Hồ Chí Minh, Việt Nam'

# no need to URI encode query, module does that for you
results = geocoder.geocode(query)

print(u'%f;%f;%s;%s' % (results[0]['geometry']['lat'],
                        results[0]['geometry']['lng'],
                        results[0]['components']['country_code'],
                        results[0]['annotations']['timezone']['name']))
# 45.797095;15.982453;hr;Europe/Belgrade
