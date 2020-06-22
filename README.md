# kondoboard-cron

This repo contains the extract, transform, load application for Kondoboard, a platform that allows Lambda School students to browse, save, and track job postings during the job hunt. 

## Table of Contents
- [Diagrams](#diagrams)
- [Install](#install) 
- [Usage](#usage)
- [Testing](#testing)
- [API](#api)
- [License](#license)


## Diagrams
We created C4 diagrams to communicate the software architecture

### Context
![Context](./diagrams/kondo_context.svg)
### Container
![Container](./diagrams/kondo_container.svg)

## Install  
```
pip install -r requirements.txt
```
## Usage  
```
uvicorn main:app --reload
```  
## Testing  
```
pytest
```  
## API

[FastAPI - Swagger documentation](http://kondoboard-ds-environment.eba-u7c3zdzn.us-east-1.elasticbeanstalk.com/docs)  
 
## License
![License](./LICENSE/)