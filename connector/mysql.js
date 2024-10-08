const mysql = require("mysql2");
const util = require("util")
const path = require('path');
const envFilePath = path.resolve(__dirname, `../connector/.env`);
const logger = require('../logger/logger')
require('dotenv').config({path:envFilePath});

const mySqlLoanTapeDbConfig = {
    host: process.env.MYSQL_HOST_LOAN_TAPE,
    port: process.env.MYSQL_PORT_LOAN_TAPE,
    user: process.env.MYSQL_USER_LOAN_TAPE,
    password: process.env.MYSQL_PASSWORD_LOAN_TAPE,
    database: process.env.MYSQL_DATABASE_LOAN_TAPE,
    connectionLimit: process.env.MYSQL_CONNECTION_LIMIT_LOAN_TAPE
}

const mySqlProdDBConfig = {
    host: process.env.MYSQL_HOST_PROD,
    port: process.env.MYSQL_PORT_PROD,
    user: process.env.MYSQL_USER_PROD,
    password: process.env.MYSQL_PASSWORD_PROD,
    database: process.env.MYSQL_DATABASE_PROD,
    connectionLimit: process.env.MYSQL_CONNECTION_LIMIT_PROD
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

  beginTransaction: async function (conn) {
    return new Promise(async (resolve, reject) => {
      try {
        const beginTransaction = util.promisify(conn.beginTransaction).bind(conn);
        await beginTransaction();
        conn.inTransaction = true;
        resolve(conn);
      } catch (error) {
        conn.release();
        logger.error("Error occured in beginTransaction:", error)
        reject(error);
      }
    });
  },

  query: async function (queryStr, params = [], location = '', conn = {}) {
    return new Promise(async (resolve, reject) => {
      try {
        conn = Object.keys(conn).length === 0 ? await this.getConnections(location) : conn
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

    
  commit: async function (conn) {
    return new Promise(async (resolve, reject) => {
      try {
        const commit = util.promisify(conn.commit).bind(conn);
        await commit();
        conn.inTransaction = false;
        conn.release();
        resolve();
      } catch (error) {
        logger.error("Error occured in commit function:", error)
        await this.rollback(conn);
        reject(error);
      }
    });
  },


  rollback: async function (conn) {
    return new Promise(async (resolve, reject) => {
      try {
        const rollback = util.promisify(conn.rollback).bind(conn);
        let res = await rollback();
        conn.inTransaction = false;
        conn.release();
        resolve(res);
      } catch (error) {
        logger.error("Error occured in rollback function:", error)
        reject(error);
      }
    });
  }
};

module.exports = _mysql
