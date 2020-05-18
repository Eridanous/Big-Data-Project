# Big-Data-Project
Big Data Analysis using Apache Spark (MapReduce + SQL)


Τα δεδομένα που χρησιμοιήθηκαν είναι πραγματικά και αφορούν σε διαδρομές taxi στην
Νέα Υόρκη. Οι δοθείσες διαδρομές των taxi έγιναν από τον Ιανουάριο εώς το Ιούνιο του 2015
και υπάρχουν διαθέσιμες online στο παρακάτω link:
https://data.cityofnewyork.us/Transportation/2015-Yellow-Taxi-Trip-Data/ba8s-jw6u .

Θέμα 1ο: Υλοποίηση SQL ερωτημάτων για αναλυτική επεξεργασία δεδομένων 

Υπολογισμός των ερωτημάτων Q1, Q2, Q3 με δύο διαφορετικούς τρόπους:
● Γράφοντας MapReduce κώδικα χρησιμοποιώντας το RDD API του Spark
● Χρησιμοποιώντας SparkSQL και το DataFrame API.

Q1: Ποια είναι η μέση διάρκεια διαδρομής (σε λεπτά) ανά ώρα έναρξης της διαδρομής;
Ταξινομείστε το αποτέλεσμα με βάση την ώρα έναρξης σε αύξουσα σειρά.

Q2: Ποιο είναι το μέγιστο ποσό που πληρώθηκε σε μία διαδρομή για κάθε εταιρεία ταξί
(vendor);

Q3: Ποιες είναι οι 5 πιο γρήγορες κούρσες που έγιναν μετά τις 10 Μαρτίου και σε ποιούς
vendors ανήκουν;

Πιο συγκεκριμένα, εκτελούνται τα εξής:
1. Upload των txt αρχείων στο HDFS. (10 μονάδες)
2. Υλοποίηση και εκτέλεση των Q1,Q2, Q3 με χρήση:
a. MapReduce κώδικα. Η υλοποίηση τρέχει απευθείας πάνω στα
αρχεία κειμένου.
b. SparkSQL. Φορτώνουμε τα αρχεία κειμένου σε Dataframes και εκτελούμε τα
ερωτήματα με χρήση της SparkSQL.
3. Μετατρέπουμε τα αρχεία κειμένου σε αρχεία Parquet. Στη συνέχεια φορτώνουμε τα Parquet
αρχεία ως Dataframes και εκτελούμε το υποερώτημα 2b. 


Θέμα 2ο: Μελέτη απόδοσης αλγορίθμων συνένωσης στο Apache Spark

Στο δεύτερο πρόβλημα μελετήθηκαν οι διαφορετικές
υλοποιήσεις που υπάρχουν στο περιβάλλον Map-Reduce του Spark για τη συνένωση (join)
δεδομένων και συγκεκριμένα το repartition join (aka Reduce-Side join) και το broadcast join (aka Map-Side join).

