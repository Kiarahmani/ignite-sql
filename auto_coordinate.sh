for i in 2 4 8 16 32 64 128; 
do mvn package && mvn exec:java -Dexec.mainClass="com.mycompany.app.App" -Dexec.args="true 1 $i"; 
sleep 5;
done