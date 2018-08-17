alias init='export MAVEN_OPTS="-Xmx850m" && mvn package && mvn exec:java -Dexec.mainClass="com.mycompany.app.App" -Dexec.args="init"'
alias compile='mvn package && mvn exec:java -Dexec.mainClass="com.mycompany.app.App" -Dexec.args="false"'
alias coordinate='mvn package && mvn exec:java -Dexec.mainClass="com.mycompany.app.App" -Dexec.args="true"'
alias run='mvn exec:java -Dexec.mainClass="com.mycompany.app.App" -Dexec.args="none"'
