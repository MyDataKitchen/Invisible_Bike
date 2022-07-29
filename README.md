# [Invisible Bikes Explorer](https://invisible-bikes.com/)

**A Youbike monitoring app which requests data from official Youbike API to identify bike lending / returning patterns and observe abnormal situations occurred from different bike stations.**

## Demo Account
Website URL: https://invisible-bikes.com/ <br />
Monitoring dashboard URL: https://invisible-bikes.com/dashboard/ <br />

![Screenshot 2022-07-28 at 3 42 55 PM](https://user-images.githubusercontent.com/88612132/181449880-f90c7cc1-6df4-4484-9479-bdae4af5d1a2.png)

## Table of Contents
* [Data Pipeline](#Data-Pipeline)
* [MySQL Schema](#MySQL-Schema)
* [Server Structure](#Server-Structure)
* [Features](#Features)
* [Technologies](#Technologies)

## Data Pipeline & Architecture
Separated crawler, data pipeline, and web server into different microservice to increase availability.

![Untitled Diagram (11)](https://user-images.githubusercontent.com/88612132/181450500-114e2169-30fd-402c-bae7-13ae170712af.png)

## MySQL Schema

![drawSQL-export-2022-07-29_08_34](https://user-images.githubusercontent.com/88612132/181659251-eef5f723-0a95-48a5-83c9-3be33bc1af2f.png)

## Technologies

#### Parallel Processing
- Modin
- Ray

#### Dashboard
- Streamlit
- Plotly
- Mapbox

#### Data pipeline
- Pandas
- Python Schedule

#### Database
- MySQL
- MongoDB

#### Cloud Service (AWS)
- EC2
- RDS
- S3

#### Cloud Service (GCP)
- Compute Engine

#### Networking
- HTTPS
- SSL
- Domain Name System (DNS)
- Nginx

#### Test
- pytest

#### Others
- Version Control: Git & GitHub
- Scrum: Trello
- Linter: pylint

#### Data Source
- [DATA.GOV.TW](https://data.gov.tw/)

## Features
#### Youbike Stations Monitoring Dashborad
![](https://user-images.githubusercontent.com/88612132/181695915-d33a4cb6-42a3-4f8a-857d-d3eb951a4996.gif)

#### Data Pipeline Dashboard
![](https://user-images.githubusercontent.com/88612132/181697237-bfcec2ce-7419-4440-bc28-77cb69fc823c.gif)

## Contact
Alfred Loh @ jiayong0226@gmail.com


