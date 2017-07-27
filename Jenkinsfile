#!/usr/bin/env groovy

// Helper methods/vars

def gitBranch = ''
def godevImage = 'quay.io/deis/go-dev:v1.1.0'
def goPkg = 'github.com/coreos/etcd-operator'

// discern between target branch and PR commits
def isMaster = { String branch ->
    branch == "remotes/origin/master"
}

// shell wrapper adding ansicolor
def sh = { cmd ->
	wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {
		sh cmd
	}
}

def registries = [
  acr: [
    staging: [
        name: 'hcpintacr.azurecr.io',
        repository: 'hcpint',
        creds: [[ $class: 'UsernamePasswordMultiBinding',
            credentialsId: 'HCPintACRcreds',
            usernameVariable: 'REGISTRY_USERNAME',
            passwordVariable: 'REGISTRY_PASSWORD',
        ]],
    ],
  ],
]

def dockerLogin = { Map registry ->
    sh """
        docker login -u="\${REGISTRY_USERNAME}" -p="\${REGISTRY_PASSWORD}" ${registry.name}
    """
}

// Pipeline begin

node('master') {
    def registry = registries.acr.staging
    env.IMAGE_REGISTRY = registry.name
    env.IMAGE_ORG = registry.repository

    stage ('Checkout') {
        checkout scm
        gitBranch = sh(returnStdout: true, script: 'git describe --all').trim()
    }

    stage ('Bootstrap') {
        sh "docker run -v $WORKSPACE:/go/src/${goPkg} -w /go/src/${goPkg} ${godevImage} glide install --strip-vendor"
    }

    // only run unit tests for now since we don't have CoreOS' e2e infrastructure set up
    stage('Test') {
        sh "docker run -v $WORKSPACE:/go/src/${goPkg} -w /go/src/${goPkg} ${godevImage} go test -v ./pkg/..."
    }

    stage('Build and Push') {
        withCredentials(registry.creds) {
            dockerLogin(registry)
            sh "docker run -v $WORKSPACE:/go/src/${goPkg} -w /go/src/${goPkg} -v /var/run/docker.sock:/var/run/docker.sock -e IMAGE=${env.IMAGE_REGISTRY}/${env.IMAGE_ORG}/etcd-operator ${godevImage} hack/build/operator/build"
        }
    }

}
