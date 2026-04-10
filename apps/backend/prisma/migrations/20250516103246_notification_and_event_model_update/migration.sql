/*
  Warnings:

  - Added the required column `scanUrl` to the `Event` table without a default value. This is not possible if the table is not empty.

*/
-- CreateEnum
CREATE TYPE "NotificationType" AS ENUM ('EVENTCREATED', 'ACTIVEEVENT', 'EVENTCOMPLETION', 'UPCOMINGREMINDER');

-- AlterTable
ALTER TABLE "Event" ADD COLUMN     "scanUrl" TEXT NOT NULL;

-- AlterTable
ALTER TABLE "Notification" ADD COLUMN     "type" "NotificationType" NOT NULL DEFAULT 'EVENTCREATED';
