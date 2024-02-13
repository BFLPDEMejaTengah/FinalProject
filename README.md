# BFLP IT Bootcamp Data Management Final Project ft. HACKTIV8
Colaborators:
1. Andi Hafiidh
2. Bimo Suryo Utomo
3. Lenny Raufi Syafitri
4. Mukhlizar Nirwan Samsuri
5. Raihan Mufid Setiadi


# How to SETUP run the project
1. use command `docker compose up -d` in terminal
2. Make sure the container is running well in your docker
   ![image](https://github.com/BFLPDEMejaTengah/FinalProject/assets/82045772/dffa5bbb-b7a1-45e5-ad4f-9dfb4e07ae89)
   
4. Access the Airflow dashboard `http://localhost:8080/`
5. Establishing Airflow connection to PostgreSQL
  - From admin menu in Navbar
  - Select `Connections`
  - Create new connection with this configuration (Don't forget to test up your connection)
    ![image](https://github.com/BFLPDEMejaTengah/FinalProject/assets/82045772/95f1ae94-a8df-449b-8bb6-691469b8dc07)
    
5. Setting up ***PostgreSQL** with configuration bellow
   ![image](https://github.com/BFLPDEMejaTengah/FinalProject/assets/82045772/7d3d18ea-df71-4e56-82df-5092a1bed4ef)
   ![image](https://github.com/BFLPDEMejaTengah/FinalProject/assets/82045772/70ebf15e-1fa1-4d99-a9cd-c9e171945507)

   For password you can fill it with `airflow` and for port you can refer the port on your docker airflow postgress port (usually `5434`)
   
6. Setting Up Complete

# Run The Dags
1. You can run the Dags from Airflow dashoard, you should see your dag file and you can run it by pressing `Play` Button
![image](https://github.com/BFLPDEMejaTengah/FinalProject/assets/82045772/a65cb826-5396-4097-9328-3ea11335b026)

2. You can see your project  process from graph at Airflow dashboard
![image](https://github.com/BFLPDEMejaTengah/FinalProject/assets/82045772/a3a32f1b-b32a-4ae6-b6bf-6ea20112868e)
![image](https://github.com/BFLPDEMejaTengah/FinalProject/assets/82045772/36925a87-6fe3-4e20-a95d-721c771c9c76)

3. You can check the file by yoursef to see if your code and automation works well

# Cenneting to Tableau
1. Download Java 8 JDBC driver from `https://jdbc.postgresql.org/download/`
2. Copy `.jar` file to following folder (or you may have created it manually) `C:\Program FIles\Tableau\Drivers`
3. Connect Tableu to PostgreSQL server by selecting connection to server in Tableu startup and select ***PostgreSQL***

 
