import requests
import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("weather_data.log"),
        logging.StreamHandler()
    ]
)

# Load environment variables from .env file
load_dotenv()

# Fetch credentials and API key from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
API_KEY = os.getenv("API_KEY")

# Set up the MySQL connection
try:
    db = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = db.cursor()
    logging.info("Successfully connected to the database.")
except mysql.connector.Error as err:
    logging.error(f"Error connecting to database: {err}")
    exit(1)

# Define cities
cities = [
    "Abidjan", "Abu Dhabi", "Abuja", "Accra", "Addis Ababa", "Ahmedabad", "Aleppo", "Alexandria", "Algiers", 
    "Almaty", "Amman", "Amsterdam", "Anchorage", "Andorra la Vella", "Ankara", "Antananarivo", "Apia", 
    "Arnold", "Ashgabat", "Asmara", "Asuncion", "Athens", "Auckland", "Avarua", "Baghdad", "Baku", "Bamako", 
    "Banda Aceh", "Bandar Seri Begawan", "Bandung", "Bangkok", "Bangui", "Banjul", "Barcelona", "Barranquilla", 
    "Basrah", "Basse-Terre", "Basseterre", "Beijing", "Beirut", "Bekasi", "Belem", "Belgrade", "Belmopan", 
    "Belo Horizonte", "Bengaluru", "Berlin", "Bern", "Bishkek", "Bissau", "Bogota", "Brasilia", "Bratislava", 
    "Brazzaville", "Bridgetown", "Brisbane", "Brussels", "Bucharest", "Budapest", "Buenos Aires", "Bujumbura", 
    "Bursa", "Busan", "Cairo", "Cali", "Caloocan", "Camayenne", "Canberra", "Cape Town", "Caracas", "Casablanca", 
    "Castries", "Cayenne", "Charlotte Amalie", "Chengdu", "Chennai", "Chicago", "Chisinau", "Chittagong", 
    "Chongqing", "Colombo", "Conakry", "Copenhagen", "Cordoba", "Curitiba", "Daegu", "Daejeon", "Dakar", "Dallas", 
    "Damascus", "Dar es Salaam", "Delhi", "Denver", "Dhaka", "Dili", "Djibouti", "Dodoma", "Doha", "Dongguan", 
    "Douala", "Douglas", "Dubai", "Dublin", "Durban", "Dushanbe", "Faisalabad", "Fort-de-France", "Fortaleza", 
    "Freetown", "Fukuoka", "Funafuti", "Gaborone", "George Town", "Georgetown", "Gibraltar", "Gitega", "Giza", 
    "Guadalajara", "Guangzhou", "Guatemala City", "Guayaquil", "Gujranwala", "Gustavia", "Gwangju", "Hamburg", 
    "Hanoi", "Harare", "Havana", "Helsinki", "Ho Chi Minh City", "Hong Kong", "Honiara", "Honolulu", "Houston", 
    "Hyderabad", "Hyderabad", "Ibadan", "Incheon", "Isfahan", "Islamabad", "Istanbul", "Izmir", "Jaipur", 
    "Jakarta", "Jeddah", "Jerusalem", "Johannesburg", "Juarez", "Juba", "Kabul", "Kaduna", "Kampala", "Kano", 
    "Kanpur", "Kaohsiung", "Karachi", "Karaj", "Kathmandu", "Kawasaki", "Kharkiv", "Khartoum", "Khulna", "Kigali", 
    "Kingsburg", "Kingston", "Kingstown", "Kinshasa", "Kobe", "Kolkata", "Kota Bharu", "Kowloon", "Kuala Lumpur", 
    "Kumasi", "Kuwait", "Kyiv", "Kyoto", "La Paz", "Lagos", "Lahore", "Libreville", "Lilongwe", "Lima", "Lisbon", 
    "Ljubljana", "Lome", "London", "Los Angeles", "Luanda", "Lubumbashi", "Lusaka", "Luxembourg", "Macau", 
    "Madrid", "Majuro", "Makassar", "Malabo", "Male", "Mamoudzou", "Managua", "Manama", "Manaus", "Manila", 
    "Maputo", "Maracaibo", "Maracay", "Mariehamn", "Marigot", "Maseru", "Mashhad", "Mbabane", "Mecca", "Medan", 
    "Medellin", "Medina", "Melbourne", "Mexico City", "Miami", "Minsk", "Mogadishu", "Monaco", "Monrovia", 
    "Montevideo", "Montreal", "Moroni", "Moscow", "Mosul", "Multan", "Mumbai", "Muscat", "N'Djamena", "Nagoya", 
    "Nairobi", "Nanchong", "Nanjing", "Nassau", "Nay Pyi Taw", "New York", "Niamey", "Nicosia", "Nouakchott", 
    "Noumea", "Novosibirsk", "Nuku'alofa", "Nur-Sultan", "Nuuk", "Oranjestad", "Osaka", "Oslo", "Ottawa", 
    "Ouagadougou", "Pago Pago", "Palembang", "Palo Alto", "Panama", "Papeete", "Paramaribo", "Paris", "Perth", 
    "Philadelphia", "Phnom Penh", "Phoenix", "Podgorica", "Port Louis", "Port Moresby", "Port of Spain", 
    "Port-Vila", "Port-au-Prince", "Porto Alegre", "Porto-Novo", "Prague", "Praia", "Pretoria", "Pristina", 
    "Puebla", "Pune", "Pyongyang", "Quezon City", "Quito", "Rabat", "Rawalpindi", "Recife", "Reykjavik", "Riga", 
    "Rio de Janeiro", "Riyadh", "Road Town", "Rome", "Roseau", "Saint George's", "Saint Helier", "Saint John's", 
    "Saint Peter Port", "Saint Petersburg", "Saint-Denis", "Saint-Pierre", "Saipan", "Salvador", "San Antonio", 
    "San Diego", "San Francisco", "San Jose", "San Juan", "San Marino", "San Salvador", "Sanaa", 
    "Santa Cruz de la Sierra", "Santiago", "Santo Domingo", "Sao Paulo", "Sao Tome", "Sapporo", "Sarajevo", 
    "Seattle", "Semarang", "Seoul", "Shanghai", "Sharjah", "Shenzhen", "Singapore", "Skopje", "Sofia", 
    "South Tangerang", "Soweto", "Stockholm", "Sucre", "Surabaya", "Surat", "Suva", "Sydney", "Tabriz", 
    "Taipei", "Tallinn", "Tangerang", "Tarawa", "Tashkent", "Tbilisi", "Tegucigalpa", "Tehran", "Tel Aviv", 
    "Thimphu", "Tianjin", "Tijuana", "Tirana", "Tokyo", "Toronto", "Torshavn", "Tripoli", "Tunis", 
    "Ulan Bator", "Vaduz", "Valencia", "Valletta", "Vancouver", "Victoria", "Vienna", "Vientiane", "Vilnius", 
    "Warsaw", "Washington", "Wellington", "Willemstad", "Windhoek", "Wuhan", "Xi'an", "Yamoussoukro", "Yangon", 
    "Yaounde", "Yekaterinburg", "Yerevan", "Yokohama", "Zagreb"
]

# Create table if not exists
def create_table():
    query = """
    CREATE TABLE IF NOT EXISTS city_weather (
        id INT AUTO_INCREMENT PRIMARY KEY,
        city VARCHAR(100),
        country VARCHAR(100),
        temperature FLOAT,
        feels_like FLOAT,
        humidity INT,
        pressure INT,
        weather_description VARCHAR(255),
        wind_speed FLOAT,
        chance_of_rain FLOAT,
        sunrise DATETIME,
        sunset DATETIME,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """
    try:
        cursor.execute(query)
        db.commit()
        logging.info("Table `city_weather` is ready.")
    except mysql.connector.Error as err:
        logging.error(f"Error creating table: {err}")

# Fetch weather data for a city
def fetch_weather(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    try:
        response = requests.get(url)
        response.raise_for_status()  
        logging.info(f"Data fetched successfully for {city}")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred for {city}: {http_err}")
    except requests.exceptions.RequestException as err:
        logging.error(f"Error fetching data for {city}: {err}")
    return None

# Save weather data to the database
def save_to_db(data):
    query = """
    INSERT INTO city_weather (
        city, country, temperature, feels_like, humidity, pressure, 
        weather_description, wind_speed, chance_of_rain, sunrise, sunset
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'])
    sunset_time = datetime.fromtimestamp(data['sys']['sunset'])
    chance_of_rain = data.get('rain', {}).get('1h', 0.0)  # Defaults to 0.0 if rain data is absent
    values = (
        data['name'],
        data['sys']['country'],
        data['main']['temp'],
        data['main']['feels_like'],
        data['main']['humidity'],
        data['main']['pressure'],
        data['weather'][0]['description'],
        data['wind']['speed'],
        chance_of_rain,
        sunrise_time,
        sunset_time
    )
    try:
        cursor.execute(query, values)
        db.commit()
        logging.info(f"Data saved to database for {data['name']}")
    except mysql.connector.Error as err:
        logging.error(f"Error saving data for {data['name']} to the database: {err}")

# Main script
create_table() 

# Fetch and save weather data for each city
for city in cities:
    weather_data = fetch_weather(city)
    if weather_data:
        save_to_db(weather_data)

logging.info("All data processing completed.")

# Close database connection
cursor.close()
db.close()
logging.info("Database connection closed.")
