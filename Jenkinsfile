pipeline {
    agent any

    environment {
        REPO_URL = "http://${env.MAVEN_URL}/repository/maven-snapshots/"
        REPO_ID = "snapshots"
    }

    tools {
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