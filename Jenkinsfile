pipeline {

  agent { label 'small' }
  environment {
    imagename_dev = "10.3.7.221:5000/common"
    imagename_staging = "10.3.7.241:5000/common"
    registryCredential = 'docker-registry'
    dockerImage = ''
  }

  stages {

    stage('DEV Git clone') {
        when { branch "k8s-dev" }
        steps {
          script {
          git branch: "k8s-dev",
              url: 'https://git.indocresearch.org/pilot/common.git',
              credentialsId: 'lzhao'
            }
        }
    }

    stage('DEV Unit test') {
      when { branch "k8s-dev" }
      steps {
        withCredentials([
        string(credentialsId:'VAULT_TOKEN', variable: 'VAULT_TOKEN'),
        string(credentialsId:'VAULT_URL', variable: 'VAULT_URL'),
        file(credentialsId:'VAULT_CRT', variable: 'VAULT_CRT')
      ]) {
          sh """
          export VAULT_TOKEN=${VAULT_TOKEN}
          export VAULT_URL=${VAULT_URL}
          export VAULT_CRT=${VAULT_CRT}
          pip3 install virtualenv
          /home/indoc/.local/bin/virtualenv -p python3 venv
          . venv/bin/activate
          pip3 install -r common/requirements.txt -r tests/test_requirements.txt
          pytest
          """
        }
      }
    }

    stage('DEV Publish package to GitLab') {
      when { branch "k8s-dev" }
      steps {
        withCredentials([usernamePassword(credentialsId:'pilot-gitlab-registry', usernameVariable: 'PILOT_DEPLOY_USERNAME', passwordVariable: 'PILOT_DEPLOY_TOKEN')]) {
          sh """
          pip3 install virtualenv
          /home/indoc/.local/bin/virtualenv -p python3 venv
          . venv/bin/activate
          pip install twine
          python setup.py sdist bdist_wheel
          TWINE_USERNAME=${PILOT_DEPLOY_USERNAME} TWINE_PASSWORD=${PILOT_DEPLOY_TOKEN} python -m twine upload --repository-url https://git.indocresearch.org/api/v4/projects/158/packages/pypi dist/*`grep 'version=".*"' setup.py | grep -o -E '[0-9]+.[0-9]+.[0-9]+'`*
          """
        }
      }
    }

    stage('STAGING Git clone') {
        when { branch "k8s-staging" }
        steps {
          script {
          git branch: "k8s-staging",
              url: 'https://git.indocresearch.org/pilot/common.git',
              credentialsId: 'lzhao'
            }
        }
    }

  }

  post {
      failure {
        slackSend color: '#FF0000', message: "Build Failed! - ${env.JOB_NAME} ${env.BUILD_NUMBER}  (<${env.BUILD_URL}|Open>)", channel: 'jenkins-dev-staging-monitor'
      }
  }

}
