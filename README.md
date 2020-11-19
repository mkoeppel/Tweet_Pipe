# Tweet_Pipe
Tweet_Pipe is an example for an extract-transform-load pipeline. It uses Docker-containers to move Twitter-API extracted tweets from a MongoDB to a PostgresDB, performs sentiment analysis of the content and provides cumulative summaries with a metabase dashboard.

## How to use:
- start by cloning the repository
- get access token and consumer api from twitter
  - paste in the appropriate place in credentials.py

- set your keywords to select tweets about a particular topic, be entering them in line 69 of the get_tweets script:
  - stream.filter(track=['put your terms in here', 'use a list structure'], languages=['en'])
    - languages could be changed, however the sentiment analysis is supported only for english right now
- In the Tweet_Pipe folder build docker pipeline by entering in a terminal shell:
  - docker-compose build
  - docker-compose up (or up -d if you want to suppress the output)

  - see its progress by typing:
    - docker-compose logs
- or to see active containers:
    - docker ps

- in case you want to stop the pipeline:
  - docker-compose kill (stops everything)
  - docker kill <process> (stops individual containers only)
