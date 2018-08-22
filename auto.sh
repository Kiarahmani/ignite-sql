rm results.txt
touch results.txt
for i in 2 4 8 16 32 64 128;  
do mvn package && mvn exec:java -Dexec.mainClass="com.mycompany.app.App" -Dexec.args="false 1 $i"; 
done