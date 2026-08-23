pipeline {
    agent { label 'java17' }

    environment {
        REPO_URL = "http://${env.MAVEN_URL}/repository/maven-snapshots/"
        REPO_ID = "snapshots"
    }

    tools {
        jdk   'temurin-17'
        maven 'M3'
    }

    stages {
        stage('构建并发布') {
            steps {
                script {
                    script {
                        sh '''
                            echo '============================== 构建并发布 =============================='
                            mvn clean deploy -DaltDeploymentRepository=${REPO_ID}::default::${REPO_URL}
                        '''
                    }
                }
            }
        }
    }
}