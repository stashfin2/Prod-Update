const { createLogger, format, transports } = require('winston');
const { combine, timestamp, printf, errors, colorize } = format;

const myFormat = printf(({ level, message, timestamp, stack }) => {
  return `${timestamp} ${level}: ${stack || message}`;
});

const logger = createLogger({
  level: 'info',
  format: combine(
    colorize(), // Add color to the log level
    timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    errors({ stack: true }), // This allows stack trace to be logged
    myFormat
  ),
  transports: [
    new transports.Console(),
    new transports.File({ filename: 'app.log', format: combine( // Remove color for file transport
      timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
      errors({ stack: true }),
      printf(({ level, message, timestamp, stack }) => {
        return `${timestamp} ${level}: ${stack || message}`;
      })
    )})
  ],
});

module.exports = logger;
