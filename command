alias init='export MAVEN_OPTS="-Xmx850m" && mvn package && mvn exec:java -Dexec.mainClass="com.mycompany.app.App" -Dexec.args="init"'
alias follow='git pull && mvn package && mvn exec:java -Dexec.mainClass="com.mycompany.app.App" -Dexec.args="false 1 2"'
alias coordinate='git pull && mvn package && mvn exec:java -Dexec.mainClass="com.mycompany.app.App" -Dexec.args="true 1 2"'
alias run='mvn exec:java -Dexec.mainClass="com.mycompany.app.App" -Dexec.args="none"'
