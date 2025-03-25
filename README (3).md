
# Football match predictor 1.0

Based on historical data: match results, teams positions, teams goals scored and conceded, teams forms deep learning model was trained for predicting upcoming matches. Based on predictions algorithm creates the most profitable bets from requsted odds. Program uses Apache Airflow for scheduling tasks.

## Assumptions:

1. All data is stored in PostgreSQL database

2. Apache Airflow is used for scheduling tasks

3. Docker is used for containerization

4. Historical data is collected from top European Leagues by daily requesting endpoint: v3.football.api-sports.io

5. Deep Learning model trained on historical data and updated once a month

6. Based on predictions request bets from endpoint: v3.football.api-sports.io and get most profitable bets

7. After weekend get recent results and check bets accuracy

8. Bets summary available on FastAPI endpoint : /summary, bet details available on FastAPI endpoint: /{bet_number}

## Airflow schedule:


### Historical:
1. Dag for requesting historical matches data 

### Daily
| Monday  | Tuesday  |Wednesday| Thursday | Friday   | Saturday | Sunday  |
|---------|----------|---------|----------|----------|----------|---------|
| Get results  | Get results   | Get results  | Colect odds for upcoming matches   | Colect odds for upcoming matches  | Get results   | |
|     |      | Predict upcoming matches  |     | Create bets for upcoming weekend   |     | |
 

### Monthly:

1. Update Deep Learning model with recent results

2. Check if new seasons are available

## Screenshots
