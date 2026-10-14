module.exports = {
  testEnvironment: 'node',
  transform: {},
  coverageDirectory: 'coverage',
  coverageReporters: ['text-summary', 'html'],
  collectCoverageFrom: [
    'docs/app/**/*.js',
    '!docs/app/shared/vendor/**'
  ]
};
