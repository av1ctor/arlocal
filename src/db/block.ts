import { Knex } from 'knex';
import { Utils } from '../utils/utils';

export class BlockDB {
  private connection: Knex;

  constructor(connection: Knex) {
    this.connection = connection;
  }

  async getOne() {
    return this.connection.select('*').from('blocks');
  }

  async getByIndepHash(indepHash: string) {
    return (await this.connection.queryBuilder().select('*').from('blocks').where('id', '=', indepHash).limit(1))[0];
  }

  async mine(height: number, previous: string, txs: string[]) {
    try {
      const id = Utils.randomID(64);

      await this.connection
        .insert({
          id,
          height,
          mined_at: Date.now(),
          previous_block: previous,
          txs: JSON.stringify(txs),
          extended: '',
        })
        .into('blocks');

      return id;
    } catch (error) {
      console.error({ error });
    }
  }
  async getLastBlock() {
    try {
      return (await this.connection('blocks').orderBy('created_at', 'desc').limit(1))[0];
    } catch (error) {
      console.error({ error });
    }
  }

  async getByHeight(height: number) {
    return (await this.connection.queryBuilder().select('*').from('blocks').where('height', '=', height).limit(1))[0];
  }

  /**
   *
   * @param id Genesis block ID/indep_hash
   */
  async insertGenesis(id: string) {
    try {
      await this.connection
        .insert({
          id,
          height: 0,
          mined_at: Date.now(),
          previous_block: '',
          txs: JSON.stringify([]),
          extended: '',
        })
        .into('blocks');
    } catch (error) {
      console.error({ error });
    }
  }
}
