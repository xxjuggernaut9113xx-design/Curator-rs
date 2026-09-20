const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.join(__dirname, '..');
const iconPath = (name) => path.join(root, 'desktop', 'icons', name);

test('Curator ships a complete PNG, multi-size ICO, and ICNS icon set', () => {
  const png = fs.readFileSync(iconPath('icon.png'));
  assert.deepEqual([...png.subarray(0, 8)], [137, 80, 78, 71, 13, 10, 26, 10]);
  assert.equal(png.readUInt32BE(16), 256);
  assert.equal(png.readUInt32BE(20), 256);

  const ico = fs.readFileSync(iconPath('icon.ico'));
  assert.equal(ico.readUInt16LE(0), 0);
  assert.equal(ico.readUInt16LE(2), 1);
  const count = ico.readUInt16LE(4);
  const sizes = new Set();
  for (let index = 0; index < count; index += 1) {
    const offset = 6 + index * 16;
    const width = ico[offset] || 256;
    const height = ico[offset + 1] || 256;
    sizes.add(`${width}x${height}`);
    assert.equal(ico.readUInt16LE(offset + 6), 32);
  }
  for (const size of ['16x16', '32x32', '48x48', '64x64', '128x128', '256x256']) {
    assert.ok(sizes.has(size), `ICO is missing ${size}`);
  }

  const icns = fs.readFileSync(iconPath('icon.icns'));
  assert.equal(icns.subarray(0, 4).toString('ascii'), 'icns');
  assert.equal(icns.readUInt32BE(4), icns.length);
  const chunks = new Set();
  for (let offset = 8; offset < icns.length;) {
    const length = icns.readUInt32BE(offset + 4);
    assert.ok(length >= 8 && offset + length <= icns.length, 'invalid ICNS chunk length');
    chunks.add(icns.subarray(offset, offset + 4).toString('ascii'));
    offset += length;
  }
  for (const chunk of ['ic07', 'ic08', 'ic09', 'ic10', 'ic11', 'ic12', 'ic13', 'ic14']) {
    assert.ok(chunks.has(chunk), `ICNS is missing ${chunk}`);
  }
});

test('native editions share the validated icon family', () => {
  assert.equal(fs.existsSync(path.join(root, 'desktop', 'icons', 'icon.png')), true);
  assert.equal(fs.existsSync(path.join(root, 'desktop', 'icons', 'icon.ico')), true);
});
