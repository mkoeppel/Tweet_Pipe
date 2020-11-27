# Tweet_Pipe
Tweet_Pipe is an example for an airflow-scheduled ETL pipeline. It uses Docker-containers to extract tweets from Twitter-API, then stores them in a MongoDB, performs sentiment analysis of the content, followed by storage in a PostgresDB, and provides cumulative summaries with a metabase dashboard. .

## How to use:
- start by cloning the repository
- get access token and consumer api from twitter
  - paste in the appropriate place in credentials.py

- set your keywords to select tweets about a particular topic, by entering them in the TwitterListener-Class into the keyword list (from line 45 on) and in line 69 of the get_tweets script:
  - keyword = None
        for key in ['put your terms in here', 'use a list structure']
  - stream.filter(track=['put your terms in here', 'use a list structure'], languages=['en'])
    - languages could be changed, however the sentiment analysis is supported only for english right now
- In the Tweet_Pipe folder build docker pipeline by entering in a terminal shell:
  - docker-compose build
  - docker-compose up (or up -d if you want to suppress the output)

  - see its progress by typing:
    - docker-compose logs
  - or to see active containers:
      - docker ps
- in your webbrowser go to http://0.0.0.0:8081/admin/ to see the pipeline working in airflow
- and to http://localhost:3000/ for the metabase dashboard

- in case you want to stop the pipeline:
  - docker-compose kill (stops everything)
  - docker kill <process> (stops individual containers only)
