
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos import PartitionKey
import json
import datetime
import os
from random import randint
import base64
from flask import Flask
from flask_restful import reqparse, Api, Resource

# Variables van verbinding
HOST = os.environ['DB_HOST']
KEY = os.environ['DB_KEY']
DATABASE_NAME = os.environ['DATABASE_NAME']
CONTAINER_NAAM = os.environ['CONTAINER_NAAM']

def default_actions(host, key, database_name, container_name, partition_name):
    '''Connect naar Azure, maakt database aan en maakt container aan

    Keyword Arguments:
    host(string) -- Host waar je naartoe verbindt.
    key(string) -- Geheime sleutel van de database.
    database_name(string) -- Naam van database die aangemaakt moet worden.
    container_name(string) -- Naam van container die aangemaakt moet worden.
    partition_name(string) -- Naam van partitie_key die bij de container hoort.

    Returns:
    container -- Container waar je vervolgens naartoe kan schrijven.
    '''

    # Connecten naar Azure
    client = cosmos_client.CosmosClient(host,{'masterKey': key})

    # Maken en ophalen van database en container
    client.create_database_if_not_exists(id=database_name)
    database = client.get_database_client(database_name)
    database.create_container_if_not_exists(container_name, partition_key=PartitionKey(path=partition_name))
    container = database.get_container_client(container_name)

    return container

def get_count(container, container_name):
    '''Haalt count op van aantal items. Dit is belangrijk om te weten welk
    id je moet ophogen

    Keyword Arugments:
    container(class) -- Output van vorige functie
    container_name(string) -- Naam van functie. Hoort bij container.

    Returns:
    count(int) -- Aantal ID's in container.
    '''

    output = container.query_items('SELECT VALUE COUNT(1) FROM {0} c'.format(container_name), enable_cross_partition_query = True)
    for item in output:
        count = (json.dumps(item))
    
    return int(count)

def new_customer(container, container_name, klant_naam, postcode, huisnummer):
    '''Voegt klant toe aan de database

    Keyword Arguments:
    container(class) -- Output van vorige functie
    container_name(string) -- Naam van functie. Hoort bij container.
    klant_naam(sting) -- Volledige naam van de klant, voorbeeld van formaat: Ralph van Leeuwen
    postcode(string) -- Postcode van klant, voorbeeld van formaat: 3437JN
    huisnummer(string) -- Huisnummer van klant, voorbeeld van formaat: 40

    Returns:
    None
    '''

    volgnummer = str(get_count(container, 'klantgegevens') + 1)

    container.upsert_item({
            'id': volgnummer,
            'klantNaam': klant_naam,
            'postCode': postcode,
            'huisNummer': huisnummer,
            'coupon': []
        }
    )

def check_doublecustomer(container, container_name, klant_naam, postcode, huisnummer):
    '''Gaat het volgende na:
    - Is de klant te vinden met de NAW gegevens die meegegeven worden.
    - Is de klant meer dan een keer te vinden met de NAW gegevens die meegegeven worden.

    0 -- Klant is niet gevonden
    1 -- Klant is gevonden en komt maar 1x voor.
    2 of meer -- Klant is gevonden maar komt meer dan 1x voor.
    
    Keyword Arguments:
    container(class) -- Output van vorige functie
    container_name(string) -- Naam van functie. Hoort bij container.
    klant_naam(sting) -- Volledige naam van de klant, voorbeeld van formaat: Ralph van Leeuwen
    postcode(string) -- Postcode van klant, voorbeeld van formaat: 3437JN
    huisnummer(string) -- Huisnummer van klant, voorbeeld van formaat: 40

    Returns:
    count(int) -- Aantal keer dat de klant gevonden is.
    '''
    query = '''SELECT VALUE COUNT(1) FROM {0} k WHERE 
    k.klantNaam = '{1}' AND
    k.postCode = '{2}' AND
    k.huisNummer = '{3}'
    '''.format(container_name, klant_naam, postcode, huisnummer)

    output = container.query_items(query, enable_cross_partition_query = True)
    
    for item in output:
        count = int((json.dumps(item)))

    return count


def delete_customer(container, container_name, klant_naam, postcode, huisnummer, klant_id = 0):
    '''Verwijderd klant van het systeem op basis
    van een zoek actie.

    Keyword Arguments:
    container(class) -- Output van vorige functie
    container_name(string) -- Naam van functie. Hoort bij container.
    klant_naam(sting) -- Volledige naam van de klant, voorbeeld van formaat: Ralph van Leeuwen
    postcode(string) -- Postcode van klant, voorbeeld van formaat: 3437JN
    huisnummer(string) -- Huisnummer van klant, voorbeeld van formaat: 40
    klant_id(string) -- Optioneel voor als er 2 klanten zijn met de zelfde NAW gegevens.

    Returns:
    Melding(string) -- Melding van wat er gebeurd is
    '''
    
    # Aanspreken van functie die controleert of er dubbele klanten zijn of geen klanten zijn.
    klant_count = check_doublecustomer(container, container_name, klant_naam, postcode, huisnummer)

    # Acties gebaseerd op of de klant gevonden is, dubbel is en een ID heeft.
    if klant_count == 0:
        return "De klant gegevens zijn niet gevonden in het systeem."
    elif klant_count > 1 and klant_id == 0:
        return "De klant is dubbel gevonden en er is geen ID waarde meegegeven."
    elif klant_count == 1:
        query = '''SELECT * FROM {0} k WHERE 
        k.klantNaam = '{1}' AND
        k.postCode = '{2}' AND
        k.huisNummer = '{3}'
        '''.format(container_name, klant_naam, postcode, huisnummer)
    elif klant_count > 1 and klant_id != 0:
        query = '''SELECT * FROM {0} k WHERE 
        k.id = '{4}'
        '''.format(container_name, klant_naam, postcode, huisnummer, klant_id)

    # Als er daadwerkelijk een query gevonden is om uit te voeren voer deze dan uit
    items = container.query_items(query, enable_cross_partition_query = True)
    
    # Het daadwerkelijk verwijderen.
    for item in items:
        container.delete_item(item, partition_key=klant_naam)

    return "Items verwijderd"

def add_visit(container, container_name, klant_naam, postcode, huisnummer, winkelnaam,  klant_id = 0):
    '''Voegt een bezoek aan een winkel toe

    Keyword Arguments:
    container(class) -- Output van vorige functie
    container_name(string) -- Naam van functie. Hoort bij container.
    klant_naam(sting) -- Volledige naam van de klant, voorbeeld van formaat: Ralph van Leeuwen
    postcode(string) -- Postcode van klant, voorbeeld van formaat: 3437JN
    huisnummer(string) -- Huisnummer van klant, voorbeeld van formaat: 40
    winkelnaam(string) -- Naam van winkel die bezocht wordt.
    klant_id(string) -- Optioneel voor als er 2 klanten zijn met de zelfde NAW gegevens.

    Returns:
    None
    '''

    # Aanspreken van functie die controleert of er dubbele klanten zijn of geen klanten zijn.
    klant_count = check_doublecustomer(container, container_name, klant_naam, postcode, huisnummer)

    # Later te gebruiken
    datum = str(datetime.datetime.now()) 

    # Acties gebaseerd op of de klant gevonden is, dubbel is en een ID heeft.
    if klant_count == 0:
        return "De klant gegevens zijn niet gevonden in het systeem."
    elif klant_count > 1 and klant_id == 0:
        return "De klant is dubbel gevonden en er is geen ID waarde meegegeven."
    elif klant_count == 1:
        query = '''SELECT * FROM {0} k WHERE 
        k.klantNaam = '{1}' AND
        k.postCode = '{2}' AND
        k.huisNummer = '{3}'
        '''.format(container_name, klant_naam, postcode, huisnummer)
    elif klant_count > 1 and klant_id != 0:
        query = '''SELECT * FROM {0} k WHERE 
        k.id = '{4}'
        '''.format(container_name, klant_naam, postcode, huisnummer, klant_id)

    # Als er daadwerkelijk een query gevonden is om uit te voeren voer deze dan uit
    items = container.query_items(query, enable_cross_partition_query = True)

    # Omzetten naar 1 item
    for item in items:
        klant = (json.dumps(item))

    tag_id = randint(100000, 100000000)

    # Formateren van nieuwe JSON
    klant_toevoegen = {
        "Locatie": winkelnaam,
        "datum": datum,
        "coupon": str(tag_id),
    }
    klant_json = json.loads(klant)
    
    klant_json['coupon'].append(klant_toevoegen)

    items = container.query_items(query, enable_cross_partition_query = True)

    # Vervangen van nieuwe json
    for item in items:
        container.replace_item(item=item, body=klant_json)

    return tag_id

def get_customer(container, container_name, klant_naam, postcode, huisnummer, klant_id = 0):
    '''Haalt klant op uit winkel op basis van NAW gegevens.

    Keyword Arguments:
    container(class) -- Output van vorige functie
    container_name(string) -- Naam van functie. Hoort bij container.
    klant_naam(sting) -- Volledige naam van de klant, voorbeeld van formaat: Ralph van Leeuwen
    postcode(string) -- Postcode van klant, voorbeeld van formaat: 3437JN
    huisnummer(string) -- Huisnummer van klant, voorbeeld van formaat: 40
    klant_id(string) -- Optioneel voor als er 2 klanten zijn met de zelfde NAW gegevens.

    Returns:
    klant(json) -- Gegevens van klant.
    '''

    # Aanspreken van functie die controleert of er dubbele klanten zijn of geen klanten zijn.
    klant_count = check_doublecustomer(container, container_name, klant_naam, postcode, huisnummer)

    # Acties gebaseerd op of de klant gevonden is, dubbel is en een ID heeft.
    if klant_count == 0:
        return "De klant gegevens zijn niet gevonden in het systeem."
    elif klant_count > 1 and klant_id == 0:
        return "De klant is dubbel gevonden en er is geen ID waarde meegegeven."
    elif klant_count == 1:
        query = '''SELECT * FROM {0} k WHERE 
        k.klantNaam = '{1}' AND
        k.postCode = '{2}' AND
        k.huisNummer = '{3}'
        '''.format(container_name, klant_naam, postcode, huisnummer)
    elif klant_count > 1 and klant_id != 0:
        query = '''SELECT * FROM {0} k WHERE 
        k.id = '{4}'
        '''.format(container_name, klant_naam, postcode, huisnummer, klant_id)

    # Als er daadwerkelijk een query gevonden is om uit te voeren voer deze dan uit
    items = container.query_items(query, enable_cross_partition_query = True)

    # Omzetten naar 1 item
    for item in items:
        klant = (json.dumps(item))

    return (json.loads(klant))

def use_coupon(container, container_name, klant_naam, postcode, huisnummer, coupon, klant_id = 0):
    '''Gebruikt een coupon

    Keyword Arguments:
    container(class) -- Output van vorige functie
    container_name(string) -- Naam van functie. Hoort bij container.
    klant_naam(sting) -- Volledige naam van de klant, voorbeeld van formaat: Ralph van Leeuwen
    postcode(string) -- Postcode van klant, voorbeeld van formaat: 3437JN
    huisnummer(string) -- Huisnummer van klant, voorbeeld van formaat: 40
    coupon(string) -- Coupon nummer
    klant_id(string) -- Optioneel voor als er 2 klanten zijn met de zelfde NAW gegevens.

    Returns:
    None
    '''

    # Aanspreken van functie die controleert of er dubbele klanten zijn of geen klanten zijn.
    klant_count = check_doublecustomer(container, container_name, klant_naam, postcode, huisnummer)

    # Acties gebaseerd op of de klant gevonden is, dubbel is en een ID heeft.
    if klant_count == 0:
        return "De klant gegevens zijn niet gevonden in het systeem."
    elif klant_count > 1 and klant_id == 0:
        return "De klant is dubbel gevonden en er is geen ID waarde meegegeven."
    elif klant_count == 1:
        query = '''SELECT * FROM {0} k WHERE 
        k.klantNaam = '{1}' AND
        k.postCode = '{2}' AND
        k.huisNummer = '{3}'
        '''.format(container_name, klant_naam, postcode, huisnummer)
    elif klant_count > 1 and klant_id != 0:
        query = '''SELECT * FROM {0} k WHERE 
        k.id = '{4}'
        '''.format(container_name, klant_naam, postcode, huisnummer, klant_id)

    # Als er daadwerkelijk een query gevonden is om uit te voeren voer deze dan uit
    items = container.query_items(query, enable_cross_partition_query = True)
    message = 'Coupon niet gevonden'

    # Omzetten naar 1 item
    for item in items:
        klant = (json.dumps(item))

    klant_json = json.loads(klant)
    i = 0
    for entry in klant_json['coupon']:
        if entry['coupon'] == str(coupon):
            klant_json['coupon'].remove(entry)
            message = 'Coupon gevonden en gebruikt.'
        i = 0 + 1

    items = container.query_items(query, enable_cross_partition_query = True)

    # Vervangen van nieuwe json
    for item in items:
        container.replace_item(item=item, body=klant_json)

    return message
    
# Functies om het een en ander te controleren, nergeer deze.
#container = default_actions(HOST, KEY, DATABASE_NAME, 'klantgegevens', '/klantNaam')
#print(check_doublecustomer(container, 'klantgegevens', 'Cornelis Stuurman', '3333WR', '396'))
#print(get_count(container, 'klantgegevens')
#print(check_doublecustomer(container, 'klantgegevens2', 'Corrie Stuurman', '3333FR', '398'))    

# Het vullen van de database met algemene informatie (runnen na legen van database)
container = default_actions(HOST, KEY, DATABASE_NAME, CONTAINER_NAAM, '/klantNaam')

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('naam')
parser.add_argument('postcode')
parser.add_argument('huisnummer')
parser.add_argument('winkelnaam')
parser.add_argument('coupon')
parser.add_argument('klantid')

class status(Resource):
    def get(self):
        return {'status': 'ok'}

class NieuweKlant(Resource):
    def post(self):
        args = parser.parse_args()
        naam = args['naam']
        postcode = args['postcode']
        huisnummer = args['huisnummer']

        container = default_actions(HOST, KEY, DATABASE_NAME, CONTAINER_NAAM, '/klantNaam')
        new_customer(container, CONTAINER_NAAM, naam, postcode, huisnummer)
        return {'status': 'ok'}

class KlantInfo(Resource):
    def get(self, naam, postcode, huisnummer):

        container = default_actions(HOST, KEY, DATABASE_NAME, CONTAINER_NAAM, '/klantNaam')
        klant = get_customer(container, CONTAINER_NAAM, naam, postcode, huisnummer)

        return klant

    def delete(self, naam, postcode, huisnummer):
        container = default_actions(HOST, KEY, DATABASE_NAME, CONTAINER_NAAM, '/klantNaam')
        klant = delete_customer(container, CONTAINER_NAAM, naam, postcode, huisnummer)

        return klant

class NieuwBezoek(Resource):
    def post(self):
        args = parser.parse_args()
        naam = args['naam']
        postcode = args['postcode']
        huisnummer = args['huisnummer']
        winkelnaam = args['winkelnaam']

        container = default_actions(HOST, KEY, DATABASE_NAME, CONTAINER_NAAM, '/klantNaam')
        visit = add_visit(container, CONTAINER_NAAM, naam, postcode, huisnummer, winkelnaam)

        return visit

class GebruikCoupon(Resource):
    def post(self):
        args = parser.parse_args()
        naam = args['naam']
        postcode = args['postcode']
        huisnummer = args['huisnummer']
        coupon = args['coupon']

        container = default_actions(HOST, KEY, DATABASE_NAME, CONTAINER_NAAM, '/klantNaam')
        couponresult = use_coupon(container, CONTAINER_NAAM, naam, postcode, huisnummer, coupon)

        return couponresult

class DubbelKlantInfo(Resource):
    def get(self, naam, postcode, huisnummer, klantid):
        container = default_actions(HOST, KEY, DATABASE_NAME, CONTAINER_NAAM, '/klantNaam')
        klant = get_customer(container, CONTAINER_NAAM, naam, postcode, huisnummer, klant_id)

        return klant

    def delete(self, naam, postcode, huisnummer, klantid):
        container = default_actions(HOST, KEY, DATABASE_NAME, CONTAINER_NAAM, '/klantNaam')
        klant = delete_customer(container, CONTAINER_NAAM, naam, postcode, huisnummer, klant_id)

        return klant

class DubbelNieuwBezoek(Resource):
    def post(self):
        args = parser.parse_args()
        naam = args['naam']
        postcode = args['postcode']
        huisnummer = args['huisnummer']
        winkelnaam = args['winkelnaam']
        klant_id = args['klantid']

        container = default_actions(HOST, KEY, DATABASE_NAME, CONTAINER_NAAM, '/klantNaam')
        visit = add_visit(container, CONTAINER_NAAM, naam, postcode, huisnummer, winkelnaam, klant_id)

        return visit

class DubbelGebruikCoupon(Resource):
    def post(self):
        args = parser.parse_args()
        naam = args['naam']
        postcode = args['postcode']
        huisnummer = args['huisnummer']
        coupon = args['coupon']
        klant_id = args['klantid']

        container = default_actions(HOST, KEY, DATABASE_NAME, CONTAINER_NAAM, '/klantNaam')
        couponresult = use_coupon(container, CONTAINER_NAAM, naam, postcode, huisnummer, coupon, klant_id)

        return couponresult

api.add_resource(status, '/api/status')
api.add_resource(NieuweKlant, '/api/nieuweklant')
api.add_resource(KlantInfo, '/api/klantinfo/<naam>/<postcode>/<huisnummer>')
api.add_resource(NieuwBezoek, '/api/nieuwbezoek')
api.add_resource(GebruikCoupon, '/api/gebruikcoupon')
api.add_resource(DubbelKlantInfo, '/api/klantinfo/<naam>/<postcode>/<huisnummer>/<klantid>')
api.add_resource(DubbelNieuwBezoek, '/api/dubbel/nieuwbezoek')
api.add_resource(DubbelGebruikCoupon, '/api/dubbel/gebruikcoupon')

# Start App
if __name__ == '__main__':
    app.run()
