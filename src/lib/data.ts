import { getCollection, type CollectionEntry } from 'astro:content';

export type Design = CollectionEntry<'designs'>;
export type Concept = CollectionEntry<'concepts'>;

/** Designs in the order they were written: the list reads like a log. */
export async function allDesigns(): Promise<Design[]> {
  const designs = await getCollection('designs');
  return designs.sort(
    (a, b) => a.data.date.getTime() - b.data.date.getTime() || a.data.title.localeCompare(b.data.title),
  );
}

export async function allConcepts(): Promise<Concept[]> {
  const concepts = await getCollection('concepts');
  return concepts.sort((a, b) => a.data.title.localeCompare(b.data.title));
}

/** concept id → the designs that lean on it, in list order. */
export function usedIn(designs: Design[]): Map<string, Design[]> {
  const map = new Map<string, Design[]>();
  for (const d of designs) {
    for (const c of d.data.concepts) {
      const list = map.get(c.id) ?? [];
      list.push(d);
      map.set(c.id, list);
    }
  }
  return map;
}

export const isoDate = (d: Date) => d.toISOString().slice(0, 10);
