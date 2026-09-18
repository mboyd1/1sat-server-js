import { Controller, Get, Route } from "tsoa";
import { readPool } from "../db";

@Route("api/stats")
export class StatsController extends Controller {
    @Get("")
    public async getStats(): Promise<{ indexer: string, height: number }> {
        this.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate')
        const { rows } = await readPool.query(`SELECT * FROM progress`)
        const results: any = {}
        rows.forEach((row: any) => {
            results[row.indexer] = row.height
        })
        
        return results
    }
}