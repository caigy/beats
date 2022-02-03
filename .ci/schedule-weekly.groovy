@Library('apm@current') _

pipeline {
  agent none
  environment {
    NOTIFY_TO = credentials('notify-to')
    PIPELINE_LOG_LEVEL = 'INFO'
  }
  options {
    timeout(time: 1, unit: 'HOURS')
    buildDiscarder(logRotator(numToKeepStr: '20', artifactNumToKeepStr: '20'))
    timestamps()
    ansiColor('xterm')
    disableResume()
    durabilityHint('PERFORMANCE_OPTIMIZED')
  }
  triggers {
    cron('H H(1-2) * * 0')
  }
  stages {
    stage('Weekly beats builds') {
      steps {
<<<<<<< HEAD
        runBuild(quietPeriod: 0, job: 'Beats/beats/master')
        runBuild(quietPeriod: 1000, job: 'Beats/beats/7.16')
        runBuild(quietPeriod: 2000, job: 'Beats/beats/7.15')
=======
        runBuilds(quietPeriodFactor: 1000, branches: ['main', '8.<minor>', '7.<minor>', '7.<next-minor>'])
>>>>>>> 237937085 (use main default branch (#29710))
      }
    }
  }
  post {
    cleanup {
      notifyBuildResult(prComment: false)
    }
  }
}

def runBuild(Map args = [:]) {
  def jobName = args.job
  build(quietPeriod: args.quietPeriod, job: jobName, parameters: [booleanParam(name: 'awsCloudTests', value: true)], wait: false, propagate: false)
}
