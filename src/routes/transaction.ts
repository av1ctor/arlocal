import Arweave from 'arweave';
import Router from 'koa-router';
import mime from 'mime';
import { formatTransaction, TransactionDB } from '../db/transaction';
import { DataDB } from '../db/data';
import { Utils } from '../utils/utils';
import { TransactionType } from '../faces/transaction';
import { Bundle } from 'arbundles';
import { WalletDB } from '../db/wallet';
import { b64UrlToBuffer, bufferTob64Url, hash } from '../utils/encoding';
import { ChunkDB } from '../db/chunks';
import { Next } from 'koa';
import Transaction from 'arweave/node/lib/transaction';
import { generateTransactionChunks } from '../utils/merkle';
import { Chunk } from '../faces/chunk';

export const pathRegex = /^\/?([a-z0-9-_]{43})/i;

let transactionDB: TransactionDB;
let dataDB: DataDB;
let walletDB: WalletDB;
let chunkDB: ChunkDB;
let oldDbPath: string;
let connectionSettings: string;
const FIELDS = [
  'id',
  'last_tx',
  'owner',
  'tags',
  'target',
  'quantity',
  'data_root',
  'data_size',
  'reward',
  'signature',
];

export async function txAnchorRoute(ctx: Router.RouterContext) {
  const txs = await ctx.connection.select('id').from('blocks').orderBy('created_at', 'desc').limit(1);
  if (txs.length) {
    ctx.body = txs[0].id;
    return;
  }
  ctx.body = '';
}

export async function txRoute(ctx: Router.RouterContext) {
  try {
    if (
      oldDbPath !== ctx.dbPath ||
      !transactionDB ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }
    const path = ctx.params.txid.match(pathRegex) || [];
    const transaction = path.length > 1 ? path[1] : '';

    const metadata = await transactionDB.getById(transaction);
    //ctx.logging.log(metadata);

    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not Found' };
      return;
    }

    ctx.status = 200;
    ctx.headers['accept-ranges'] = 'bytes';
    ctx.headers['content-length'] = metadata.data_size;
    ctx.body = metadata;
  } catch (error) {
    console.error({ error });
  }
}

export async function txOffsetRoute(ctx: Router.RouterContext) {
  try {
    if (
      oldDbPath !== ctx.dbPath ||
      !transactionDB ||
      !chunkDB ||
      !dataDB ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      chunkDB = new ChunkDB(ctx.connection);
      dataDB = new DataDB(ctx.dbPath);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const path = ctx.params.txid.match(pathRegex) || [];
    const transaction = path.length > 1 ? path[1] : '';

    const metadata: Transaction = await transactionDB.getById(transaction);
    //ctx.logging.log(metadata);

    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not Found' };
      return;
    }
    const chunk = await chunkDB.getByRootAndSize(metadata.data_root, +metadata.data_size);

    ctx.status = 200;
    ctx.type = 'text/plain'; // TODO: updated this in arweave gateway to app/json

    ctx.body = { offset: `${+chunk.offset + +metadata.data_size - 1}`, size: `${metadata.data_size}` };
  } catch (error) {
    console.error({ error });
  }
}

export async function txPostRoute(ctx: Router.RouterContext) {
  try {
    if (oldDbPath !== ctx.dbPath || !dataDB || !walletDB) {
      dataDB = new DataDB(ctx.dbPath);
      walletDB = new WalletDB(ctx.connection);
      chunkDB = new ChunkDB(ctx.connection);

      oldDbPath = ctx.dbPath;
    }
    const data = ctx.request.body as unknown as TransactionType;
    const owner = bufferTob64Url(await hash(b64UrlToBuffer(data.owner)));

    const wallet = await walletDB.getWallet(owner);
    const calculatedReward = Math.round((+(data.data_size || '0') / 1000) * 65595508);

    if (!wallet) {
      ctx.status = 404;
      ctx.body = { status: 404, msg: `Wallet not found` };
      return ctx;
    }
    if (wallet.balance < calculatedReward) {
      ctx.status = 410;
      ctx.body = { status: 410, msg: "You don't have enough tokens" };
      return ctx;
    }

    //ctx.logging.log('post', data);

    let bundleFormat = '';
    let bundleVersion = '';

    for (const tag of data.tags) {
      const name = Utils.atob(tag.name);
      const value = Utils.atob(tag.value);
      if (name === 'Bundle-Format') bundleFormat = value;
      if (name === 'Bundle-Version') bundleVersion = value;
    }

    if (bundleFormat === 'binary' && bundleVersion === '2.0.0') {
      // ANS-104

      const createTxsFromItems = async (buffer: Buffer) => {
        const bundle = new Bundle(buffer);

        const items = bundle.items;
        for (let i = 0; i < items.length; i++) {
          const item = items[i];

          const res = await txPostRoute({
            ...ctx,
            connection: ctx.connection,
            dbPath: ctx.dbPath,
            logging: ctx.logging,
            network: ctx.network,
            request: {
              ...ctx.request,
              body: {
                id: bundle.get(i).id,
                bundledIn: data.id,
                ...item.toJSON(),
              },
            },
            txInBundle: true,
          });

          if(res.status >= 400) {
            return {status: res.status, body: res.body};
          }
        }

        return {status: 200, body: ''};
      };

      if (data.data) {
        const buffer = Buffer.from(data.data, 'base64');
        const res = await createTxsFromItems(buffer);
        if(res.status >= 400) {
          ctx.status = res.status;
          ctx.body = res.body;
          return;
        }
      } else {
        (async () => {
          let lastOffset = 0;
          let chunks: Chunk[];
          while (+data.data_size !== lastOffset) {
            chunks = await chunkDB.getRoot(data.data_root);
            const firstChunkOffset = +chunks[0]?.offset || 0;
            const lastChunk = chunks[chunks.length - 1];
            const lastChunkLength = lastChunk ? b64UrlToBuffer(lastChunk.chunk).byteLength : 0;
            lastOffset = +chunks[chunks.length - 1]?.offset - firstChunkOffset + lastChunkLength || 0;
          }

          const chunk = chunks.map((ch) => Buffer.from(b64UrlToBuffer(ch.chunk)));

          const buffer = Buffer.concat(chunk);
          const res = await createTxsFromItems(buffer);
          if(res.status >= 400) {
            ctx.status = res.status;
            ctx.body = res.body;
            return;
          }
        })();
      }
    }

    // for tx without chunk
    // create the chunk, to prevent offset error on tx/:offset endpoint
    if (data.data && !ctx.txInBundle) {
      // create tx chunks if not exists
      const chunk = await chunkDB.getByRootAndSize(data.data_root, +data.data_size);

      if (!chunk) {
        // get data from data db
        const dataBuf = b64UrlToBuffer(data.data);

        const nChunk = await generateTransactionChunks(dataBuf);
        // make chunks offsets unique
        const lastOffset = await chunkDB.getLastChunkOffset();

        // create all chunks
        const asyncOps = nChunk.chunks.map((_chunk, idx) => {
          const proof = nChunk.proofs[idx];
          return chunkDB.create({
            chunk: bufferTob64Url(dataBuf.slice(_chunk.minByteRange, _chunk.maxByteRange)),
            data_size: +data.data_size,
            data_path: bufferTob64Url(proof.proof),
            data_root: bufferTob64Url(nChunk.data_root),
            offset: proof.offset + lastOffset,
          });
        });

        await Promise.all(asyncOps);
      }
    }

    // BALANCE UPDATES
    if (data?.target && data?.quantity) {
      let targetWallet = await walletDB.getWallet(data.target);
      if (!targetWallet) {
        await walletDB.addWallet({
          address: data?.target,
          balance: 0,
        });

        targetWallet = await walletDB.getWallet(data.target);
      }

      if (!wallet || !targetWallet) {
        ctx.status = 404;
        ctx.body = { status: 404, error: `Wallet not found` };
        return;
      }
      if (wallet?.balance < +data.quantity + +data.reward) {
        ctx.status = 403;
        ctx.body = { status: 403, error: `you don't have enough funds to send ${data.quantity}` };
        return;
      }
      await walletDB.incrementBalance(data.target, +data.quantity);
      await walletDB.decrementBalance(wallet.address, +data.quantity);
    }

    await dataDB.insert({ txid: data.id, data: data.data });

    const tx = formatTransaction(data);
    tx.created_at = new Date().toISOString();
    tx.height = ctx.network.blocks;

    await ctx.connection.insert(tx).into('transactions');

    let index = 0;
    for (const tag of data.tags) {
      const name = Utils.atob(tag.name);
      const value = Utils.atob(tag.value);

      //ctx.logging.log(name, value);

      await ctx.connection
        .insert({
          index,
          tx_id: tx.id,
          name,
          value,
        })
        .into('tags');

      index++;
    }

    // Don't charge wallet for arbundles Data-Items
    // @ts-ignore
    if (!ctx.txInBundle) {
      const fee = +data.reward > calculatedReward ? +data.reward : calculatedReward;
      await walletDB.decrementBalance(owner, +fee);
    }
    ctx.body = data;
    return ctx;
  } catch (error) {
    console.error({ error });
    ctx.status = 500;
    ctx.body = { status: 500, msg: String(error) };
    return ctx;
  }
}

export async function txStatusRoute(ctx: Router.RouterContext) {
  try {
    if (
      oldDbPath !== ctx.dbPath ||
      !transactionDB ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const path = ctx.params.txid.match(pathRegex) || [];
    const transaction = path.length > 1 ? path[1] : '';

    const metadata = await transactionDB.getById(transaction);

    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not Found !' };
      return;
    }
    if (!metadata.block) {
      ctx.body = 'Pending';
      return;
    }

    ctx.body = {
      block_height: metadata.height,
      block_indep_hash: metadata.block,
      number_of_confirmations: ctx.network.height - metadata.height,
    };
    return;
  } catch (error) {
    console.error({ error });
  }
}

export async function txFieldRoute(ctx: Router.RouterContext, next: Next) {
  try {
    if (
      oldDbPath !== ctx.dbPath ||
      !transactionDB ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const path = ctx.params.txid.match(pathRegex) || [];
    const transaction = path.length > 1 ? path[1] : '';

    const field = ctx.params.field;
    if (field.includes('.')) {
      await next();
      return;
    }
    if (!FIELDS.includes(field)) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Field Not Found !' };
      return;
    }

    const metadata = await transactionDB.getById(transaction);
    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not Found !' };
      return;
    }

    if (!metadata.block) {
      ctx.body = 'Pending';
      return;
    }

    ctx.body = metadata[field];
    return;
  } catch (error) {
    console.error({ error });
  }
}

export async function txFileRoute(ctx: Router.RouterContext) {
  try {
    if (
      oldDbPath !== ctx.dbPath ||
      !transactionDB ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const path = ctx.params.txid.match(pathRegex) || [];
    const transaction = path.length > 1 ? path[1] : '';

    const file = ctx.params.file;

    const metadata = await transactionDB.getById(transaction);
    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not Found !' };
      return;
    }

    if (!metadata.block) {
      ctx.body = 'Pending';
      return;
    }

    ctx.redirect(`http://${ctx.request.header.host}/${transaction}/${file}`);
    return;
  } catch (error) {
    console.error({ error });
  }
}

export async function txRawDataRoute(ctx: Router.RouterContext) {
  try {
    if (
      !transactionDB ||
      !dataDB ||
      oldDbPath !== ctx.dbPath ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      dataDB = new DataDB(ctx.dbPath);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const path = ctx.params.txid.match(pathRegex) || [];
    const txid = path.length > 1 ? path[1] : '';

    const metadata: TransactionType = await transactionDB.getById(txid);

    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not found' };
      return;
    }

    // Check for the data_size
    if (BigInt(metadata.data_size) > 10_000_000) {
      ctx.status = 400;
      ctx.body = { problem: 'data is too big', solution: 'use the `/chunk/:offset` routing to download bigger chunks' };
      return;
    }

    // Find the transaction data
    const {data} = await dataDB.findOne(txid);

    let buffer: Buffer;
    if (data.length === 0) {
      // then we need fetch the chunks
      const chunks = await chunkDB.getRoot(metadata.data_root);
      chunks.sort((a: Chunk, b: Chunk) => a.offset - b.offset);
      buffer = Buffer.concat(
        chunks.map(({ chunk: chunkData }) => {
          return Buffer.from(Arweave.utils.b64UrlToBuffer(chunkData));
        })
      )
    } else {
      buffer = Buffer.from(Arweave.utils.b64UrlToBuffer(data));
    }

    ctx.status = 200;
    ctx.type = Utils.tagValue(metadata.tags, 'Content-Type') || 'application/octet-stream'
    ctx.body = buffer;
  } catch (error) {
    ctx.status = 500;
    ctx.body = { error: error.message };
  }
}

export async function txDataRoute(ctx: Router.RouterContext, next: Next) {
  try {
    if (
      !transactionDB ||
      !dataDB ||
      oldDbPath !== ctx.dbPath ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      dataDB = new DataDB(ctx.dbPath);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const path = ctx.params.txid.match(pathRegex) || [];
    const txid = path.length > 1 ? path[1] : '';

    const metadata: TransactionType = await transactionDB.getById(txid);

    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not found' };
      return;
    }

    const ext = ctx.params.ext;
    const contentType = mime.getType(ext);

    // Find the transaction data
    const data = await dataDB.findOne(txid);

    if (!data || !data.data) {
      // move to next controller
      return await next();
    }

    // parse raw data to manifest
    const parsedData = Utils.atob(data.data);

    ctx.header['content-type'] = contentType;
    ctx.status = 200;
    ctx.body = parsedData;
  } catch (error) {
    ctx.status = 500;
    ctx.body = { error: error.message };
  }
}

export async function txPendingRoute(ctx: Router.RouterContext) {
  try {
    if (
      oldDbPath !== ctx.dbPath ||
      !transactionDB ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const txIds = await transactionDB.getUnminedTxs();

    ctx.status = 200;
    ctx.body = txIds;
  } catch (error) {
    console.error({ error });
    ctx.status = 500;
    ctx.body = { error: error.message };
  }
}

export async function deleteTxRoute(ctx: Router.RouterContext) {
  try {
    if (
      !transactionDB ||
      !dataDB ||
      oldDbPath !== ctx.dbPath ||
      connectionSettings !== ctx.connection.client.connectionSettings.filename
    ) {
      transactionDB = new TransactionDB(ctx.connection);
      dataDB = new DataDB(ctx.dbPath);
      oldDbPath = ctx.dbPath;
      connectionSettings = ctx.connection.client.connectionSettings.filename;
    }

    const path = ctx.params.txid.match(pathRegex) || [];
    const txid = path.length > 1 ? path[1] : '';

    const metadata: TransactionType = await transactionDB.getById(txid);

    if (!metadata) {
      ctx.status = 404;
      ctx.body = { status: 404, error: 'Not found' };
      return;
    }

    await transactionDB.deleteById(txid);

    ctx.status = 200;
    return;
  } catch (error) {
    ctx.status = 500;
    ctx.body = { error: error.message };
  }
}
