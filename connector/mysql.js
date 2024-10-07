const mysql = require("mysql2");
const path = require('path');
const envFilePath = path.resolve(__dirname, `../cron/.env`);
const logger = require('../logger/logger')
require('dotenv').config({ path: envFilePath });

const mySqlProdDBConfig = {
    host: process.env.MYSQL_HOST_SOURCE,
    port: process.env.MYSQL_PORT_SOURCE,
    user: process.env.MYSQL_USER_SOURCE,
    password: process.env.MYSQL_PASSWORD_SOURCE,
    database: process.env.MYSQL_DATABASE_SOURCE,
    connectionLimit: process.env.CONNECTION_LIMIT_SOURCE
}

const mySqlLoanTapeDbConfig = {
    host: process.env.MYSQL_HOST_DESTINATION,
    port: process.env.MYSQL_PORT_DESTINATION,
    user: process.env.MYSQL_USER_DESTINATION,
    password: process.env.MYSQL_PASSWORD_DESTINATION,
    database: process.env.MYSQL_DATABASE_DESTINATION,
    connectionLimit: process.env.CONNECTION_LIMIT_DESTINATION
}

const prodDbPool = mysql.createPool(mySqlProdDBConfig)
const loanTapeDbPool = mysql.createPool(mySqlLoanTapeDbConfig)


const _mysql = {

  getConnections: async function (location) {
    return new Promise(async (resolve, reject) => {
      try {
        let getConnection = location === 'prod' ? util.promisify(prodDbPool.getConnection).bind(prodDbPool) : util.promisify(loanTapeDbPool.getConnection).bind(loanTapeDbPool)
        const conn = await getConnection();
        resolve(conn);
      } catch (error) {
        logger.error(`Error while getting connection for ${location}:`, error)
        reject(error);
      }
    });
  },



  query: async function (queryStr, params = [], location = '') {
    return new Promise(async (resolve, reject) => {
      try {
        const conn = await this.getConnections(location)
        if(params.length > 0 && Array.isArray(params[0])){
          params = [params[0]]
        }
        const query = util.promisify(conn.query).bind(conn); // Bind conn as the context
        const res = await query(queryStr, params);
        conn.release();
        return resolve(res);
      } catch (error) {
        logger.error(`Error while querying the database for ${location}:`,error)
        reject(error);
      }
    });
  },

  end: async function () {
    return new Promise(async (resolve, reject) => {
      try {
        const end = util.promisify(pool.end).bind(pool); 
        await end();
        resolve();
      } catch (error) {
        logger.error(`Error while ending the connection:`, error)
        reject(error);
      }
    });
  },
};

module.exports = _mysql
