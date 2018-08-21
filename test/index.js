import { MongoClient } from 'mongodb';
import Cache from '..';
import assert from 'assert';
import crypto from 'crypto';
import fs from 'fs';

const uri = 'mongodb://127.0.0.1:27017/cacheman-mongo-test'
let cache;

describe('cacheman-mongo', function () {

  before(async () => {
    cache = new Cache({ host: '127.0.0.1', port: 27017, database: 'cacheman-mongo-test' });
  });

  after(async () => {
    await cache.clear()
    await cache.client.dropDatabase()
    await cache.closeMongoClient()
  });

  it('should have main methods', function () {
    assert.ok(cache.set);
    assert.ok(cache.get);
    assert.ok(cache.del);
    assert.ok(cache.clear);
  });

  it('should store items', function (done) {
    cache.set('test1', { a: 1 }, function (err) {
      if (err) return done(err);
      cache.get('test1', function (err, data) {
        if (err) return done(err);
        assert.equal(data.a, 1);
        done();
      });
    });
  });

  it('should store zero', function (done) {
    cache.set('test2', 0, function (err) {
      if (err) return done(err);
      cache.get('test2', function (err, data) {
        if (err) return done(err);
        assert.strictEqual(data, 0);
        done();
      });
    });
  });

  it('should store false', function (done) {
    cache.set('test3', false, function (err) {
      if (err) return done(err);
      cache.get('test3', function (err, data) {
        if (err) return done(err);
        assert.strictEqual(data, false);
        done();
      });
    });
  });

  it('should store null', function (done) {
    cache.set('test4', null, function (err) {
      if (err) return done(err);
      cache.get('test4', function (err, data) {
        if (err) return done(err);
        assert.strictEqual(data, null);
        done();
      });
    });
  });

  it('should delete items', function (done) {
    let value = Date.now();
    cache.set('test5', value, function (err) {
      if (err) return done(err);
      cache.get('test5', function (err, data) {
        if (err) return done(err);
        assert.equal(data, value);
        cache.del('test5', function (err) {
          if (err) return done(err);
          cache.get('test5', function (err, data) {
            if (err) return done(err);
            assert.equal(data, null);
            done();
          });
        });
      });
    });
  });

  it('should clear items', function (done) {
    const cache2 = new Cache('mongodb://127.0.0.1:27017/cacheman-mongo-test-2');
    let value = Date.now();
    cache2.set('test6', value, function (err) {
      if (err) return cache2.closeMongoClient(() => done(err))
      cache2.get('test6', function (err, data) {
        if (err) return cache2.closeMongoClient(() => done(err))
        assert.equal(data, value);
        cache2.clear(function (err) {
          if (err) return cache2.closeMongoClient(() => done(err))
          cache2.get('test6', function (err, data) {
            if (err) return cache2.closeMongoClient(() => done(err))
            assert.equal(data, null);
            cache2.closeMongoClient(done)
          });
        });
      });
    });
  });

  it('should expire key', function (done) {
    this.timeout(0);
    cache.set('test7', { a: 1 }, 1, function (err) {
      if (err) return done(err);
      setTimeout(function () {
        cache.get('test7', function (err, data) {
        if (err) return done(err);
          assert.equal(data, null);
          done();
        });
      }, 1100);
    });
  });

  it('should allow passing mongodb connection string', function (done) {
    const cache2 = new Cache(uri);
    cache2.set('test8', { a: 1 }, function (err) {
      if (err) return cache2.closeMongoClient(() => done(err))
      cache2.get('test8', function (err, data) {
        if (err) return cache2.closeMongoClient(() => done(err))
        assert.equal(data.a, 1);
        cache2.closeMongoClient(done)
      });
    });
  });

  it('should allow passing mongo db instance as first argument', function (done) {
    MongoClient.connect(uri, { useNewUrlParser: true }, function (err, mongoClient) {
      if (err) return done(err)
      const cache2 = new Cache(mongoClient.db());
      cache2.set('test9', { a: 1 }, function (err) {
        if (err) return cache2.closeMongoClient(() => done(err))
        cache2.get('test9', function (err, data) {
          if (err) return cache2.closeMongoClient(() => done(err))
          assert.equal(data.a, 1);
          mongoClient.close(done)
        });
      });
    });
  });

  it('should allow passing mongo db instance as client in object', function (done) {
    MongoClient.connect(uri, { useNewUrlParser: true }, function (err, mongoClient) {
      if (err) return done(err);
      const cache2 = new Cache({ client: mongoClient.db() });
      cache2.set('test9', { a: 1 }, function (err) {
        if (err) return cache2.closeMongoClient(() => done(err))
        cache2.get('test9', function (err, data) {
          if (err) return cache2.closeMongoClient(() => done(err))
          assert.equal(data.a, 1);
          mongoClient.close(done)
        });
      });
    });
  });

  it('should get the same value subsequently', function(done) {
    let val = 'Test Value';
    cache.set('test', 'Test Value', function() {
      cache.get('test', function(err, data) {
        if (err) return done(err);
        assert.strictEqual(data, val);
        cache.get('test', function(err, data) {
          if (err) return done(err);
          assert.strictEqual(data, val);
          cache.get('test', function(err, data) {
            if (err) return done(err);
             assert.strictEqual(data, val);
             done();
          });
        });
      });
    });
  });

  describe('cacheman-mongo compression', function () {
    let cache3
    before(async () => {
      cache3 = new Cache({ compression: true, database: 'cacheman-mongo-test' });
    });

    after(async () => {
      await cache3.clear()
      await cache3.closeMongoClient()
    });

    it('should store compressable item compressed', function (done) {
      let value = Date.now().toString();

      cache3.set('test1', new Buffer(value), function (err) {
        if (err) return done(err);
        cache3.get('test1', function (err, data) {
          if (err) return done(err);
          assert.equal(data.toString(), value);
          done();
        });
      });
    });

    it('should store non-compressable item normally', function (done) {
      let value = Date.now().toString();

      cache3.set('test2', value, function (err) {
        if (err) return done(err);
        cache3.get('test2', function (err, data) {
          if (err) return done(err);
          assert.equal(data, value);
          done();
        });
      });
    });

    it('should store large compressable item compressed', function (done) {
      let value = fs.readFileSync('./test/large.bin'), // A file larger than the 16mb MongoDB document size limit
          md5 = function(d){ return crypto.createHash('md5').update(d).digest('hex'); };

      cache3.set('test3', value, function (err) {
        if (err) return done(err);
        cache3.get('test3', function (err, data) {
          if (err) return done(err);
          assert.equal(md5(data), md5(value));
          done();
        });
      });
    });
  });
});