import mysql.connector

# database configuration
try:
    mydb = mysql.connector.connect(
        host="162.241.85.86",
        user="mlhtracc_localhost",
        password="MLHTracker@123",
        database="mlhtracc_tracker"
    )
    cursor = mydb.cursor()
    print("connection successfull")
except mysql.connector.Error as error:
    print("Error connecting to MySQL: {}".format(error))
exit()