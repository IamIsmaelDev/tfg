import datetime # Para manejar fechas y horas
import os # Para manejar archivos y carpetas
import re # Para manejar expresiones regulares
from elasticsearch import Elasticsearch # Para conectarse con Elasticsearch
import urllib3 # Para desactivar las advertencias de solicitud insegura

# Desactivar las advertencias de solicitud insegura (no recomendado para producción)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Clase que sirve para configurar la conexión con Elasticsearch
class ElasticSearchConfig:
    def __init__(self, host='localhost', port=9200, scheme='https', index_name='default_index', username=None, password=None):
        self.host = host
        self.port = port
        self.index_name = index_name
        self.scheme = scheme
        self.username = username
        self.password = password

# Clase que sirve para guardar los datos en Elasticsearch
class ElasticSearchRecorder:
    def __init__(self, config):
        self.es = Elasticsearch(
            [f'{config.scheme}://{config.host}:{config.port}'],
            http_auth=(config.username, config.password),
            verify_certs=False  # Ignorar la verificación del certificado SSL
        )
        self.index_name = config.index_name

        # Mapeo del índice para especificar el tipo de datos de cada campo en Elasticsearch
        self.index_mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {
                        "type": "date"
                    },
                    "flight_number": {
                        "type": "integer"
                    },
                    "city_to": {
                        "type": "keyword"
                    },
                    "country_to": {
                        "type": "keyword"
                    },
                    "location": {
                        "type": "geo_point"
                    },
                    "airline": {
                        "type": "keyword"
                    },
                    "people": {
                        "type": "integer"
                    },
                    "weather": {
                        "type": "keyword"
                    },
                    "state": {
                        "type": "keyword"
                    },
                    "cancelled_args": {
                        "type": "keyword"
                    }
                }
            }
        }

        # Crear el índice si no existe
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, body=self.index_mapping)

    # Método que indexa los datos en Elasticsearch (guarda los datos)
    def index_data(self, document):
        try:
            res = self.es.index(index=self.index_name, body=document)
            return True
        except Exception as e:
            print("Failed to index data: ", e)
            return False
    
    # Método que guarda los datos en Elasticsearch llamando al método index_data
    def save_data(self, document):
        return self.index_data(document)

# Clase que se encarga de procesar los logs y guardar los datos en Elasticsearch
class LogProcessor:
    def __init__(self, path_folder):
        self.path_folder = path_folder
        self.es_config = ElasticSearchConfig(
            host='localhost',  # Aquí se especifica el host
            port=9200,
            scheme='https',
            index_name='aeropuerto',
            username='elastic',
            password='*z1ZEjNNmmWlYkRFa1_k'
        )
        self.es_recorder = ElasticSearchRecorder(self.es_config)
    
    # Método que obtiene la fecha y hora en formato timestamp a partir de una línea de log
    def get_datetime_from_line(self, line):
        pattern = r'(\d{2}:\d{2}:\d{2}) (\d{2}/\d{2}/\d{4})'
        
        # Buscar coincidencias en la línea
        match = re.search(pattern, line)
        
        if match:
            time_str = match.group(1)
            date_str = match.group(2)
            
            # Crear un objeto datetime
            datetime_str = f"{date_str} {time_str}"
            datetime_obj = datetime.datetime.strptime(datetime_str, '%d/%m/%Y %H:%M:%S')
            
            # Convertir el objeto datetime a un timestamp
            timestamp = datetime_obj.strftime('%Y-%m-%dT%H:%M:%S')

            # restamos una hora para que sea la hora de España
            timestamp = datetime_obj - datetime.timedelta(hours=1)

            return timestamp
        
        else:
            return None

    
    # Método que obtiene el número de vuelo a partir de una línea de log
    def get_flight_number_from_line(self, line):
        pattern = r' \d{6} '
        match = re.search(pattern, line)

        if match:
            flight_number = match.group(0)
            return flight_number
        else:
            return None
    
    # Método que obtiene la ciudad de destino a partir de una línea de log
    def get_city_to_from_line(self, line):
        pattern = r'\[(.*?)\]'
        matches = re.findall(pattern, line)

        if matches:
            return matches[0]
        else:
            return None
    
    # Método que obtiene el país de destino a partir de una línea de log
    def get_country_to_from_line(self, line):
        pattern = r'\[(.*?)\]'
        matches = re.findall(pattern, line)

        if matches:
            return matches[1]
        else:
            return None
    
    # Método que obtiene las coordenadas de destino a partir de una línea de log.
    # Se crea un diccionario con las coordenadas de los países ya que las librerias tienen limite de peticiones.
    def get_coordinates_to_from_line(self, line):
        # crea un diccionario con todas las coordenadas de todos los paises del mundo ya que las librerias tienen un límite de peticiones
        coordinates = {
            "Afganistán": {"lat": 33.93911, "lon": 67.709953},
            "Albania": {"lat": 41.153332, "lon": 20.168331},
            "Argelia": {"lat": 28.033886, "lon": 1.659626},
            "Andorra": {"lat": 42.546245, "lon": 1.601554},
            "Angola": {"lat": -11.202692, "lon": 17.873887},
            "Antigua y Barbuda": {"lat": 17.060816, "lon": -61.796428},
            "Argentina": {"lat": -38.416097, "lon": -63.616672},
            "Armenia": {"lat": 40.069099, "lon": 45.038189},
            "Australia": {"lat": -25.274398, "lon": 133.775136},
            "Austria": {"lat": 47.516231, "lon": 14.550072},
            "Azerbaiyán": {"lat": 40.143105, "lon": 47.576927},
            "Bahamas": {"lat": 25.03428, "lon": -77.39628},
            "Baréin": {"lat": 25.930414, "lon": 50.637772},
            "Bangladés": {"lat": 23.684994, "lon": 90.356331},
            "Barbados": {"lat": 13.193887, "lon": -59.543198},
            "Bielorrusia": {"lat": 53.709807, "lon": 27.953389},
            "Bélgica": {"lat": 50.503887, "lon": 4.469936},
            "Belice": {"lat": 17.189877, "lon": -88.49765},
            "Benín": {"lat": 9.30769, "lon": 2.315834},
            "Bután": {"lat": 27.514162, "lon": 90.433601},
            "Bolivia": {"lat": -16.290154, "lon": -63.588653},
            "Bosnia y Herzegovina": {"lat": 43.915886, "lon": 17.679076},
            "Botsuana": {"lat": -22.328474, "lon": 24.684866},
            "Brasil": {"lat": -14.235004, "lon": -51.92528},
            "Brunéi": {"lat": 4.535277, "lon": 114.727669},
            "Bulgaria": {"lat": 42.733883, "lon": 25.48583},
            "Burkina Faso": {"lat": 12.238333, "lon": -1.561593},
            "Burundi": {"lat": -3.373056, "lon": 29.918886},
            "Cabo Verde": {"lat": 16.5388, "lon": -23.0418},
            "Camboya": {"lat": 12.565679, "lon": 104.990963},
            "Camerún": {"lat": 7.369722, "lon": 12.354722},
            "Canadá": {"lat": 56.130366, "lon": -106.346771},
            "República Centroafricana": {"lat": 6.611111, "lon": 20.939444},
            "Chad": {"lat": 15.454166, "lon": 18.732207},
            "Chile": {"lat": -35.675147, "lon": -71.542969},
            "China": {"lat": 35.86166, "lon": 104.195397},
            "Colombia": {"lat": 4.570868, "lon": -74.297333},
            "Comoras": {"lat": -11.6455, "lon": 43.3333},
            "Congo": {"lat": -0.228021, "lon": 15.827659},
            "Costa Rica": {"lat": 9.748917, "lon": -83.753428},
            "Croacia": {"lat": 45.1, "lon": 15.2},
            "Cuba": {"lat": 21.521757, "lon": -77.781167},
            "Chipre": {"lat": 35.126413, "lon": 33.429859},
            "República Checa": {"lat": 49.817492, "lon": 15.472962},
            "República Democrática del Congo": {"lat": -4.038333, "lon": 21.758664},
            "Dinamarca": {"lat": 56.26392, "lon": 9.501785},
            "Yibuti": {"lat": 11.825138, "lon": 42.590275},
            "Dominica": {"lat": 15.414999, "lon": -61.370976},
            "República Dominicana": {"lat": 18.735693, "lon": -70.162651},
            "Ecuador": {"lat": -1.831239, "lon": -78.183406},
            "Egipto": {"lat": 26.820553, "lon": 30.802498},
            "El Salvador": {"lat": 13.794185, "lon": -88.89653},
            "Guinea Ecuatorial": {"lat": 1.650801, "lon": 10.267895},
            "Eritrea": {"lat": 15.179384, "lon": 39.782334},
            "Estonia": {"lat": 58.595272, "lon": 25.013607},
            "Etiopía": {"lat": 9.145, "lon": 40.489673},
            "Fiyi": {"lat": -16.578193, "lon": 179.414413},
            "Finlandia": {"lat": 61.92411, "lon": 25.748151},
            "Francia": {"lat": 46.603354, "lon": 1.888334},
            "Gabón": {"lat": -0.803689, "lon": 11.609444},
            "Gambia": {"lat": 13.443182, "lon": -15.310139},
            "Georgia": {"lat": 42.315407, "lon": 43.356892},
            "Alemania": {"lat": 51.165691, "lon": 10.451526},
            "Ghana": {"lat": 7.946527, "lon": -1.023194},
            "Grecia": {"lat": 39.074208, "lon": 21.824312},
            "Granada": {"lat": 12.262776, "lon": -61.604171},
            "Guatemala": {"lat": 15.783471, "lon": -90.230759},
            "Guinea": {"lat": 9.945587, "lon": -9.696645},
            "Guinea-Bisáu": {"lat": 11.803749, "lon": -15.180413},
            "Guyana": {"lat": 4.860416, "lon": -58.93018},
            "Haití": {"lat": 18.971187, "lon": -72.285215},
            "Honduras": {"lat": 15.199999, "lon": -86.241905},
            "Hungría": {"lat": 47.162494, "lon": 19.503304},
            "Islandia": {"lat": 64.963051, "lon": -19.020835},
            "India": {"lat": 20.593684, "lon": 78.96288},
            "Indonesia": {"lat": -0.789275, "lon": 113.921327},
            "Irán": {"lat": 32.427908, "lon": 53.688046},
            "Irak": {"lat": 33.223191, "lon": 43.679291},
            "Irlanda": {"lat": 53.41291, "lon": -8.24389},
            "Israel": {"lat": 31.046051, "lon": 34.851612},
            "Italia": {"lat": 41.87194, "lon": 12.56738},
            "Jamaica": {"lat": 18.109581, "lon": -77.297508},
            "Japón": {"lat": 36.204824, "lon": 138.252924},
            "Jordania": {"lat": 30.585164, "lon": 36.238414},
            "Kazajistán": {"lat": 48.019573, "lon": 66.923684},
            "Kenia": {"lat": -0.023559, "lon": 37.906193},
            "Kiribati": {"lat": -3.370417, "lon": -168.734039},
            "Kuwait": {"lat": 29.31166, "lon": 47.481766},
            "Kirguistán": {"lat": 41.20438, "lon": 74.766098},
            "Laos": {"lat": 19.85627, "lon": 102.495496},
            "Letonia": {"lat": 56.879635, "lon": 24.603189},
            "Líbano": {"lat": 33.854721, "lon": 35.862285},
            "Lesoto": {"lat": -29.609988, "lon": 28.233608},
            "Liberia": {"lat": 6.428055, "lon": -9.429499},
            "Libia": {"lat": 26.3351, "lon": 17.228331},
            "Liechtenstein": {"lat": 47.166, "lon": 9.555373},
            "Lituania": {"lat": 55.169438, "lon": 23.881275},
            "Luxemburgo": {"lat": 49.815273, "lon": 6.129583},
            "Macedonia": {"lat": 41.608635, "lon": 21.745275},
            "Madagascar": {"lat": -18.766947, "lon": 46.869107},
            "Malaui": {"lat": -13.254308, "lon": 34.301525},
            "Malasia": {"lat": 4.210484, "lon": 101.975766},
            "Maldivas": {"lat": 3.202778, "lon": 73.22068},
            "Malí": {"lat": 17.570692, "lon": -3.996166},
            "Malta": {"lat": 35.937496, "lon": 14.375416},
            "Islas Marshall": {"lat": 7.131474, "lon": 171.184478},
            "Mauritania": {"lat": 21.00789, "lon": -10.940835},
            "Mauricio": {"lat": -20.348404, "lon": 57.552152},
            "México": {"lat": 23.634501, "lon": -102.552784},
            "Micronesia": {"lat": 7.425554, "lon": 150.550812},
            "Moldavia": {"lat": 47.411631, "lon": 28.369885},
            "Mónaco": {"lat": 43.750298, "lon": 7.412841},
            "Mongolia": {"lat": 46.862496, "lon": 103.846656},
            "Montenegro": {"lat": 42.708678, "lon": 19.37439},
            "Marruecos": {"lat": 31.791702, "lon": -7.09262},
            "Mozambique": {"lat": -18.665695, "lon": 35.529562},
            "Myanmar": {"lat": 21.913965, "lon": 95.956223},
            "Namibia": {"lat": -22.95764, "lon": 18.49041},
            "Nauru": {"lat": -0.522778, "lon": 166.931503},
            "Nepal": {"lat": 28.394857, "lon": 84.124008},
            "Países Bajos": {"lat": 52.132633, "lon": 5.291266},
            "Nueva Zelanda": {"lat": -40.900557, "lon": 174.885971},
            "Nicaragua": {"lat": 12.865416, "lon": -85.207229},
            "Níger": {"lat": 17.607789, "lon": 8.081666},
            "Nigeria": {"lat": 9.081999, "lon": 8.675277},
            "Noruega": {"lat": 60.472024, "lon": 8.468946},
            "Omán": {"lat": 21.512583, "lon": 55.923255},
            "Pakistán": {"lat": 30.375321, "lon": 69.345116},
            "Palaos": {"lat": 7.51498, "lon": 134.58252},
            "Panamá": {"lat": 8.537981, "lon": -80.782127},
            "Papúa Nueva Guinea": {"lat": -6.314993, "lon": 143.95555},
            "Paraguay": {"lat": -23.442503, "lon": -58.443832},
            "Perú": {"lat": -9.189967, "lon": -75.015152},
            "Filipinas": {"lat": 12.879721, "lon": 121.774017},
            "Polonia": {"lat": 51.919438, "lon": 19.145136},
            "Portugal": {"lat": 39.399872, "lon": -8.224454},
            "Catar": {"lat": 25.354826, "lon": 51.183884},
            "Rumanía": {"lat": 45.943161, "lon": 24.96676},
            "Rusia": {"lat": 61.52401, "lon": 105.318756},
            "Ruanda": {"lat": -1.940278, "lon": 29.873888},
            "San Cristóbal y Nieves": {"lat": 17.357822, "lon": -62.782998},
            "San Marino": {"lat": 43.94236, "lon": 12.457777},
            "San Vicente y las Granadinas": {"lat": 12.984305, "lon": -61.287228},
            "Samoa": {"lat": -13.759029, "lon": -172.104629},
            "Santo Tomé y Príncipe": {"lat": 0.18636, "lon": 6.613081},
            "Arabia Saudita": {"lat": 23.885942, "lon": 45.079162},
            "Senegal": {"lat": 14.497401, "lon": -14.452362},
            "Serbia": {"lat": 44.016521, "lon": 21.005859},
            "Seychelles": {"lat": -4.679574, "lon": 55.491977},
            "Sierra Leona": {"lat": 8.460555, "lon": -11.779889},
            "Singapur": {"lat": 1.352083, "lon": 103.819836},
            "Eslovaquia": {"lat": 48.669026, "lon": 19.699024},
            "Eslovenia": {"lat": 46.151241, "lon": 14.995463},
            "Islas Salomón": {"lat": -9.64571, "lon": 160.156194},
            "Somalia": {"lat": 5.152149, "lon": 46.199616},
            "Sudáfrica": {"lat": -30.559482, "lon": 22.937506},
            "Sudán del Sur": {"lat": 6.876991, "lon": 31.306978},
            "España": {"lat": 40.463667, "lon": -3.74922},
            "Sri Lanka": {"lat": 7.873054, "lon": 80.771797},
            "Sudán": {"lat": 12.862807, "lon": 30.217636},
            "Surinam": {"lat": 3.919305, "lon": -56.027783},
            "Suazilandia": {"lat": -26.522503, "lon": 31.465866},
            "Suecia": {"lat": 60.128161, "lon": 18.643501},
            "Suiza": {"lat": 46.818188, "lon": 8.227512},
            "Siria": {"lat": 34.802075, "lon": 38.996815},
            "Tayikistán": {"lat": 38.861034, "lon": 71.276093},
            "Tanzania": {"lat": -6.369028, "lon": 34.888822},
            "Tailandia": {"lat": 15.870032, "lon": 100.992541},
            "Timor Oriental": {"lat": -8.874217, "lon": 125.727539},
            "Togo": {"lat": 8.619543, "lon": 0.824782},
            "Tonga": {"lat": -21.178986, "lon": -175.198242},
            "Trinidad y Tobago": {"lat": 10.691803, "lon": -61.222503},
            "Túnez": {"lat": 33.886917, "lon": 9.537499},
            "Turquía": {"lat": 38.963745, "lon": 35.243322},
            "Turkmenistán": {"lat": 38.969719, "lon": 59.556278},
            "Tuvalu": {"lat": -7.109535, "lon": 177.64933},
            "Uganda": {"lat": 1.373333, "lon": 32.290275},
            "Ucrania": {"lat": 48.379433, "lon": 31.16558},
            "Emiratos Árabes Unidos": {"lat": 23.424076, "lon": 53.847818},
            "Reino Unido": {"lat": 55.378051, "lon": -3.435973},
            "Estados Unidos": {"lat": 37.09024, "lon": -95.712891},
            "Uruguay": {"lat": -32.522779, "lon": -55.765835},
            "Uzbekistán": {"lat": 41.377491, "lon": 64.585262},
            "Vanuatu": {"lat": -15.376706, "lon": 166.959158},
            "Ciudad del Vaticano": {"lat": 41.902916, "lon": 12.453389},
            "Venezuela": {"lat": 6.42375, "lon": -66.58973},
            "Vietnam": {"lat": 14.058324, "lon": 108.277199},
            "Yemen": {"lat": 15.552727, "lon": 48.516388},
            "Zambia": {"lat": -13.133897, "lon": 27.849332},
            "Zimbabue": {"lat": -19.015438, "lon": 29.154857},
            "Kosovo": {"lat": 42.602636, "lon": 20.902977},
            "Sahara Occidental": {"lat": 24.215527, "lon": -12.885834},
            "Somalilandia": {"lat": 9.411743, "lon": 45.357017},
            "Taiwán": {"lat": 23.69781, "lon": 120.960515},
            "República Turca del Norte de Chipre": {"lat": 35.2332, "lon": 33.2203},
            "Sudán del Sur": {"lat": 6.876991, "lon": 31.306978},
            "Corea del Sur": {"lat": 35.907757, "lon": 127.766922},
        }

        # guarda las coordenadas de destino para que elastic lo reconozca como geo_point
        if self.country_to in coordinates:
            coordinates = coordinates[self.country_to]
            geo_point = {
                "lat": coordinates["lat"],
                "lon": coordinates["lon"]
            }
            return geo_point
        else:
            return None
    
    def get_airline_from_line(self, line):
        pattern = r'\[(.*?)\]'
        matches = re.findall(pattern, line)

        if matches:
            return matches[2]
        else:
            return None
    
    def get_people_from_line(self, line):
        # quiero que busque el formato espacio, 3 digitos, espacio
        pattern = r' \d{3} '
        match = re.search(pattern, line)
        if match:
            people = match.group(0)
            return people
        else:
            return None
    
    def get_weather_from_line(self, line):
        pattern = r'\[(.*?)\]'
        matches = re.findall(pattern, line)

        if matches:
            return matches[3]
        else:
            return None
    
    def get_state_from_line(self, line):
        pattern = r'\[(.*?)\]'
        matches = re.findall(pattern, line)

        if matches:
            return matches[4]
        else:
            return None
    
    def get_cancelled_args_from_line(self, line):
        pattern = r'\[(.*?)\]'
        matches = re.findall(pattern, line)

        if matches:
            # puede que exista el 5 o no, si no existe devuelve None
            try:
                return matches[5]
            except:
                return None
    
    def convert_data_to_dict(self):
        document = {
            'timestamp': self.timestamp,
            'flight_number': self.flight_number,
            'city_to': self.city_to,
            'country_to': self.country_to,
            'location': self.location,
            'airline': self.airline,
            'people': self.people,
            'weather': self.weather,
            'state': self.state,
            'cancelled_args': self.cancelled_args
        }
        return document

    def process_logs(self):

        # Bucle que recorre los archivos .log de la carpeta y procesa cada línea de los logs
        for file in os.listdir(self.path_folder):
            if file.endswith(".log"):
                with open(os.path.join(self.path_folder, file), 'r') as f:
                    for line in f:
                        
                        # Obtiene la fecha y hora en formato timestamp (formato requerido por elasticsearch)
                        self.timestamp = self.get_datetime_from_line(line)

                        # Obtiene el numero de vuelo
                        self.flight_number = self.get_flight_number_from_line(line)

                        # Obtiene la ciudad de destino
                        self.city_to = self.get_city_to_from_line(line)

                        # Obtiene el país de destino
                        self.country_to = self.get_country_to_from_line(line)

                        # Obtiene las coordenadas de destino
                        self.location = self.get_coordinates_to_from_line(line)

                        # Obtiene la aerolínea
                        self.airline = self.get_airline_from_line(line)

                        # Obtiene el número de personas
                        self.people = self.get_people_from_line(line)

                        # Obtiene el tiempo
                        self.weather = self.get_weather_from_line(line)

                        # Obtiene el estado del vuelo
                        self.state = self.get_state_from_line(line)

                        # Obtiene el motivo de cancelación
                        self.cancelled_args = self.get_cancelled_args_from_line(line)

                        # Convierte los datos en un diccionario
                        document = self.convert_data_to_dict()

                        # Guarda los datos en elasticsearch
                        self.es_recorder.save_data(document)

                        print(document)

                        # pasa a la siguiente línea
                        continue

    
# Método principal que llama a las clases y métodos necesarios para que funcione el programa
def main():
    path_folder_with_logs = "/Users/ismaelabedcogollor/Documents/Proyectos/tfg/python/aeropuerto"
    lp = LogProcessor(path_folder_with_logs)
    print("Processing logs...")
    logs = lp.process_logs()
    print("Logs processed")

# Cuando se ejecuta el programa python se llama al método main inicialmente
if __name__ == "__main__":
    main()