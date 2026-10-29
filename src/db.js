// The database client (Prisma). Every file that talks to Postgres imports this one shared client.
// The tables are defined in prisma/schema.prisma.
const { PrismaClient } = require('@prisma/client');

const prisma = new PrismaClient();

module.exports = prisma;
