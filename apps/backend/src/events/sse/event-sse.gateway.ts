import { Injectable } from "@nestjs/common";
import { Response } from "express";



@Injectable()
export class EventSseGateway {
    private clients: Response[] = []

    addClient(res: Response){
        this.clients.push(res)
        res.on('close', () => {
            this.clients = this.clients.filter(client => client !== res)
        })
    }

     private sendNotification(eventType: string, data: any) {
         for (const client of this.clients) {
        try {
            if (!client.writableEnded) {
                client.write(`event: ${eventType}\ndata: ${JSON.stringify(data)}\n\n`);
            }
        } catch (error) {
            console.error('SSE write error:', error);
            this.clients = this.clients.filter(c => c !== client);
        }
    }
    }

    notifyEventActivated(data: any) {
        this.sendNotification('event-activated', data);
    }

    notifyEventClosed(data: any) {
        this.sendNotification('event-closed', data);
    }
}