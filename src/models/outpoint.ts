import { BadRequest } from 'http-errors';
export class Outpoint {
    txid: Buffer = Buffer.alloc(32);
    vout: number = 0;

    constructor(txid?: Buffer, vout?: number) {
        if(txid) this.txid = txid;
        if(vout) this.vout = vout;
    }
    
    toString() {
        return this.txid.toString('hex') + '_' + this.vout;
    }

    toBuffer() {
        return Buffer.concat([
            this.txid,
            Buffer.from(this.vout.toString(16).padStart(8, '0'), 'hex'),
        ]);
    }

    static fromString(str: string) {
        const origin = new Outpoint();
        // A bare Error surfaces as a 500. Callers sending "<txid>:0" instead of "<txid>_0"
        // generated 1,027 bogus 500s in a day; that is bad input, so answer 400.
        if(!str.match(/^[0-9a-fA-F]{64}_\d*$/)) throw new BadRequest(`invalid outpoint '${str}' (expected <64-hex txid>_<vout>)`)
        origin.txid = Buffer.from(str.slice(0, 64), 'hex');
        origin.vout = parseInt(str.slice(65), 10);
        return origin;
    }

    static fromBuffer(buf: Buffer) {
        const origin = new Outpoint();
        origin.txid = buf.slice(0, 32);
        origin.vout = parseInt(buf.slice(32).toString('hex'), 16);
        return origin;
    }

    toJSON() {
        return this.toString();
    }
}