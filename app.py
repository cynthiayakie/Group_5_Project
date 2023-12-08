from flask import Flask, request, render_template
from bs4 import BeautifulSoup
import sqlalchemy
import requests
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, select


hockeyApp = Flask("Hockey Data Scrapper")


hockeyDBEngine = create_engine('sqlite:///hockey_data.db')
hockeyDBMetadata = MetaData()

# Define the table
hockeyDataTable = Table('hockey_data', hockeyDBMetadata,
    Column('id', Integer, primary_key=True),
    Column('team', String),
    Column('year', Integer),
    Column('wins', Integer),
    Column('losses', Integer),
    Column('win_percentage', Float),
    Column('goals_for', Integer),
    Column('goals_against', Integer),
    Column('goal_difference', Integer),
)

hockeyDBMetadata.create_all(hockeyDBEngine)


@hockeyApp.route('/about')
def about():
    return render_template('about.html')


@hockeyApp.route('/scrape')
def scrapeData():
    base_url = 'https://www.scrapethissite.com/pages/forms/?page_num='
    max_pages = 100  # We added this to be able to control the number of pages we scrape through

    for page_num in range(1, max_pages + 1):
        url = base_url + str(page_num)
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        for row in soup.find_all('tr'):
            columns = row.find_all('td')
            if columns and columns[3].text.strip().isdigit() and columns[4].text.strip().isdigit():
                try:
                    with hockeyDBEngine.connect() as connection:
                        connection.execute(hockeyDataTable.insert().values(
                            team=columns[0].text.strip(),
                            year=int(columns[1].text.strip()),
                            wins=int(columns[2].text.strip()),
                            losses=int(columns[3].text.strip()),
                            win_percentage=float(columns[5].text.strip()),
                            goals_for=int(columns[6].text.strip()),
                            goals_against=int(columns[7].text.strip()),
                            goal_difference=int(columns[8].text.strip())
                        ))
                        connection.commit()
                except ValueError:
                    print(f"Skipping row due to invalid data: {columns}")

@hockeyApp.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        query = request.form['query']
        with hockeyDBEngine.connect() as connection:
            result = connection.execute(select(hockeyDataTable).where(hockeyDataTable.c.team.contains(query)))
            return render_template('results.html', data=result.fetchall())
    else:
        return render_template('search_page.html')

@hockeyApp.route('/data')
def data():
    with hockeyDBEngine.connect() as connection:
        result = connection.execute(select(hockeyDataTable))
        return render_template('data.html', data=result.fetchall())

if __name__ == '__main__':
    hockeyApp.run(debug=True)
