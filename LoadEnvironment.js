const dotenv = require('dotenv');
const fs = require('fs');
const path = require('path');

function loadEnvironment() {
    const env = process.env.NODE_ENV || 'development';
    const envFile = `.env.${env}`;
    const envPath = path.resolve(__dirname, envFile);

    if (fs.existsSync(envPath)) {
        console.log(`✅ Loading ${envFile}`);
        dotenv.config({ path: envPath });
    } else {
        console.log('⚠️ No env file found');
    }

    console.log('ALLOWED_ORIGINS:', process.env.ALLOWED_ORIGINS);
}

module.exports = loadEnvironment;