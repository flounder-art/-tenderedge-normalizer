import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

const OUTPUT_TABLE = process.env.OUTPUT_TABLE || 'tenders';

async function normalizer() {
  const start = Date.now();
  console.log('=== NORMALIZER START ===', new Date().toISOString());

  try {
    const { data: tenders, error: fetchError } = await supabase
      .from(OUTPUT_TABLE)
      .select('*');

    if (fetchError) {
      console.error('Fetch error:', fetchError.message);
      process.exit(1);
    }

    if (!tenders || tenders.length === 0) {
      console.log('No tenders to normalize');
      return;
    }

    console.log(`Processing ${tenders.length} tenders`);

    const deduped = new Map();
    tenders.forEach(t => {
      if (t.url) {
        if (!deduped.has(t.url) || new Date(t.scraped_at) > new Date(deduped.get(t.url).scraped_at)) {
          deduped.set(t.url, t);
        }
      }
    });

    const normalized = Array.from(deduped.values()).map(t => ({
      ...t,
      is_vegan: /vegan|plant-based|meat-free/.test(`${t.title} ${t.description}`.toLowerCase()),
      is_la_tagged: /council|authority|borough/.test(`${t.buyer}`.toLowerCase()),
      is_em_tagged: /leicester|nottingham|derby|lincoln/.test(`${t.buyer}`.toLowerCase()),
      is_social_care_tagged: /social care|care home|domiciliary/.test(`${t.description}`.toLowerCase()),
      is_lgr_tagged: /local government|reorganisation|lgr/.test(`${t.description}`.toLowerCase()),
      vertical: /social care|care home/.test(`${t.description}`.toLowerCase()) && /council|authority/.test(`${t.buyer}`.toLowerCase()) ? 'la_social_care' : 'other'
    }));

    console.log(`Deduplicated to ${normalized.length} unique tenders`);

    const { error: upsertError } = await supabase
      .from(OUTPUT_TABLE)
      .upsert(normalized, { onConflict: 'url' });

    if (upsertError) {
      console.error('Upsert error:', upsertError.message);
      process.exit(1);
    }

    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    console.log(`=== NORMALIZER DONE in ${elapsed}s ===`);
  } catch (err: any) {
    console.error('Normalizer fatal error:', err.message);
    process.exit(1);
  }
}

normalizer();
