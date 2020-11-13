from flask import Flask, render_template, url_for, request, escape, session, Response
import pandas as pd
import sqlalchemy

app = Flask(__name__)

con = sqlalchemy.create_engine("mysql+pymysql://root:password@localhost/trafic_aerien")

# con = sqlalchemy.create_engine("mysql+pymysql://groupe4_python:peEen8T7EgYmFBi@db4free.net/trafic_aerien")


@app.route('/')
@app.route('/home')
def hello_world():
    return 'Hello, World!'


@app.route('/best10')
def best_10():
    # Récupère les 10 destinations les plus prisées
    data = pd.read_sql_query("SELECT dest, airports.name, airports.lat, airports.lon, airports.alt, COUNT(dest) AS "
                             "Plus_prisées from flights inner join airports on flights.dest = airports.faa GROUP BY "
                             "dest ORDER BY COUNT(dest) DESC LIMIT 0,10", con=con)

    return Response(data.to_json(orient="records"), mimetype='application/json')


@app.route('/lowest10')
def lowest_10():
    # Récupère les 10 destinations les moins prisées
    data = pd.read_sql_query("SELECT dest, airports.name, airports.lat, airports.lon, airports.alt, COUNT(dest) AS "
                             "count from flights inner join airports on flights.dest = airports.faa GROUP BY "
                             "dest ORDER BY COUNT(dest) ASC LIMIT 0,10", con=con)

    return Response(data.to_json(orient="records"), mimetype='application/json')


@app.route('/airlines-dest-nb')
def airlines_dest_nb():
    # Récupère le nombre d'origines et destinations des compagnies aérienne
    data = pd.read_sql_query("SELECT count(DISTINCT origin) as 'origins', flights.carrier, airlines.name, "
                             "count(DISTINCT dest) as 'destinations' FROM flights INNER JOIN airlines ON "
                             "flights.carrier = airlines.carrier GROUP BY carrier ORDER BY count(DISTINCT dest) DESC", con=con)

    return Response(data.to_json(orient="records"), mimetype='application/json')


if __name__ == '__main__':
    app.run(debug=True)
