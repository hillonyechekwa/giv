import { Injectable, Logger } from "@nestjs/common";
import { Cron, CronExpression } from "@nestjs/schedule";
import { PrismaService } from "src/prisma/prisma.service";
import { EventStatus } from "@prisma/client";
import { EventSseGateway } from "../sse/event-sse.gateway";


@Injectable()
export class EventCronService {
    private readonly logger = new Logger(EventCronService.name)
    constructor(
        private prisma: PrismaService,
        private readonly sseGateway: EventSseGateway
    ) { }


    //TODO: remember to update this to use redis instead.
    // @Cron(CronExpression.EVERY_MINUTE) // Check every minute
    // async handleUpcomingEventsCron() {
    //     const now = new Date();

    //     // Fetch all upcoming events
    //     const events = await this.prisma.event.findMany({
    //         where: {
    //             status: EventStatus.UPCOMING,
    //         },
    //         select: { id: true, date: true, eventStartTime: true, name: true, userId: true},
    //     });

    //     // Filter events where the current time has passed the eventStartTime
    //     const toActivate = events.filter(evt => {
    //         const eventStartDateTime = new Date(
    //             `${evt.date.toISOString().slice(0, 10)}T${evt.eventStartTime.toISOString().slice(11, 19)}Z`
    //         );
    //         return eventStartDateTime <= now;
    //     });

    //     // Process in small batches to avoid database overload (throttling)
    //     const BATCH_SIZE = 10;
    //     let batchIndex = 0;

    //     const processNextBatch = async () => {
    //         const batch = toActivate.slice(batchIndex, batchIndex + BATCH_SIZE);

    //         if (batch.length === 0) {
    //             return;
    //         }

    //         // Update the status of these events to ACTIVE
    //         await this.prisma.event.updateMany({
    //             where: { id: { in: batch.map(e => e.id) } },
    //             data: { status: EventStatus.ACTIVE },
    //         });

    //         for(const event of batch){
    //             await this.prisma.notification.create({
    //                 data: {
    //                     userId: event.userId,
    //                     eventId: event.id,
    //                     title: `Event is live!`,
    //                     message: `Your event "${event.name}" is now active.`
    //                 }
    //             })
                
    //             this.sseGateway.notifyEventActivated({
    //                 userId: event.id,
    //                 id: event.id,
    //                 title: 'Event is live!',
    //                 message: `Your event "${event.name}" is now active`,
    //                 timeStamp: new Date()
    //             })
    //         }

            
                
            

    //         batchIndex += BATCH_SIZE;
    //         setTimeout(processNextBatch, 1000); // Throttling by waiting 1 second before processing the next batch
    //     };

    //     // Start the batch processing
    //     await processNextBatch();
    // }


    @Cron(CronExpression.EVERY_MINUTE)
    async handleUpcomingEventsCron() {
        const now = new Date();
        this.logger.log(`Checking upcoming events at ${now.toISOString()}`);

        try {
            const events = await this.prisma.event.findMany({
                where: { status: EventStatus.UPCOMING },
                select: { id: true, date: true, eventStartTime: true, name: true, userId: true },
            });

            const toActivate = events.filter(evt => {
                // Combine date and time correctly in UTC
                const eventDateStr = evt.date.toISOString().slice(0, 10);
                const eventTimeStr = evt.eventStartTime.toISOString().slice(11, 19);
                const eventStartDateTime = new Date(`${eventDateStr}T${eventTimeStr}Z`);
                this.logger.debug(`Event ${evt.id} start: ${eventStartDateTime.toISOString()}, now: ${now.toISOString()}`);
                return eventStartDateTime <= now;
            });

            this.logger.log(`Found ${toActivate.length} events to activate`);

            const BATCH_SIZE = 10;
            let batchIndex = 0;

            const processNextBatch = async () => {
                const batch = toActivate.slice(batchIndex, batchIndex + BATCH_SIZE);
                if (batch.length === 0) return;

                try {
                    // Update event statuses
                    await this.prisma.event.updateMany({
                        where: { id: { in: batch.map(e => e.id) } },
                        data: { status: EventStatus.ACTIVE },
                    });
                    this.logger.log(`Updated ${batch.length} events to ACTIVE`);

                    // Create notifications
                    for (const event of batch) {
                        try {
                            await this.prisma.notification.create({
                                data: {
                                    userId: event.userId,
                                    eventId: event.id,
                                    title: `Event is live!`,
                                    message: `Your event "${event.name}" is now active.`
                                }
                            });
                            this.logger.log(`Notification created for event ${event.id}`);
                        } catch (error) {
                            this.logger.error(`Error creating notification for event ${event.id}: ${error.message}`);
                        }

                        // Send SSE
                        try {
                            this.sseGateway.notifyEventActivated({
                                userId: event.userId, // Corrected from event.id
                                id: event.id,
                                title: 'Event is live!',
                                message: `Your event "${event.name}" is now active`,
                                timeStamp: new Date()
                            });
                        } catch (error) {
                            this.logger.error(`Error sending SSE for event ${event.id}: ${error.message}`);
                        }
                    }
                } catch (error) {
                    this.logger.error(`Error processing batch: ${error.message}`);
                }

                batchIndex += BATCH_SIZE;
                setTimeout(processNextBatch, 1000);
            };

            await processNextBatch();
        } catch (error) {
            this.logger.error(`Error in handleUpcomingEventsCron: ${error.message}`);
        }
    }

    // Cron job to handle events that should be closed once their eventEndTime has passed
    // @Cron(CronExpression.EVERY_HOUR) // Run every hour
    // async handleExpiredEventsCron() {
    //     const now = new Date();

    //     // Fetch active events where eventEndTime has passed
    //     const events = await this.prisma.event.findMany({
    //         where: {
    //             status: EventStatus.ACTIVE,
    //             eventEndTime: { lte: now }, // eventEndTime is less than or equal to the current time
    //         },
    //         select: { id: true, eventEndTime: true },
    //     });

    //     if (events.length === 0) {
    //         console.log('No events to close.');
    //         return;
    //     }

    //     // Process in small batches to avoid database overload (throttling)
    //     const BATCH_SIZE = 10;
    //     let batchIndex = 0;

    //     const processNextBatch = async () => {
    //         const batch = events.slice(batchIndex, batchIndex + BATCH_SIZE);

    //         if (batch.length === 0) {
    //             return;
    //         }

    //         // Update the status of these events to CLOSED
    //         await this.prisma.event.updateMany({
    //             where: { id: { in: batch.map(e => e.id) } },
    //             data: { status: EventStatus.CLOSED },
    //         });

    //         batchIndex += BATCH_SIZE;
    //         setTimeout(processNextBatch, 1000); // Throttling by waiting 1 second before processing the next batch
    //     };

    //     // Start the batch processing
    //     await processNextBatch();
    // }

     @Cron(CronExpression.EVERY_HOUR)
    async handleExpiredEventsCron() {
        const now = new Date();
        this.logger.log(`Checking expired events at ${now.toISOString()}`);

        try {
            const events = await this.prisma.event.findMany({
                where: {
                    status: EventStatus.ACTIVE,
                    eventEndTime: { lte: now },
                },
                select: { 
                    id: true, 
                    name: true,
                    userId: true,
                    eventEndTime: true 
                },
            });

            this.logger.log(`Found ${events.length} events to close`);

            const BATCH_SIZE = 10;
            let batchIndex = 0;

            const processNextBatch = async () => {
                const batch = events.slice(batchIndex, batchIndex + BATCH_SIZE);
                if (batch.length === 0) return;

                try {
                    // Update event statuses
                   await this.prisma.event.updateMany({
                        where: { id: { in: batch.map(e => e.id) } },
                        data: { status: EventStatus.CLOSED },
                    });
                    this.logger.log(`Closed ${batch.length} events`);

                    // Create notifications and send SSE
                    for (const event of batch) {
                        try {
                            await this.prisma.notification.create({
                                data: {
                                    userId: event.userId,
                                    eventId: event.id,
                                    title: `Event ended`,
                                    message: `Your event "${event.name}" has concluded.`
                                }
                            });
                            this.logger.log(`Created closure notification for event ${event.id}`);
                        } catch (error) {
                            this.logger.error(`Notification error for event ${event.id}: ${error.message}`);
                        }

                        try {
                            this.sseGateway.notifyEventClosed({
                                userId: event.userId,
                                id: event.id,
                                title: 'Event ended',
                                message: `Your event "${event.name}" has concluded`,
                                timeStamp: new Date()
                            });
                        } catch (error) {
                            this.logger.error(`SSE error for event ${event.id}: ${error.message}`);
                        }
                    }
                } catch (error) {
                    this.logger.error(`Batch processing error: ${error.message}`);
                }

                batchIndex += BATCH_SIZE;
                setTimeout(processNextBatch, 1000);
            };

            await processNextBatch();
        } catch (error) {
            this.logger.error(`Expired events cron error: ${error.message}`);
        }
    }
}
