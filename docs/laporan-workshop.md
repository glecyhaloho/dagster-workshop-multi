# Laporan Workshop: dagster-workshop-multi

## 1. Ringkasan

Workshop ini menggunakan [Dagster](https://dagster.io) dengan pola multi-container:
satu container Docker per pipeline, masing-masing menjalankan gRPC code server
sendiri, terdaftar ke satu webserver/daemon pusat via `workspace.yaml`. Ada 3
pipeline dasar (`pipeline_products`, `pipeline_fx`, `pipeline_ml`) yang menulis
ke satu warehouse Postgres bersama, plus 1 pipeline tambahan yang saya bangun
sendiri untuk bagian capstone (`pipeline_reporting`).

## 2. Exercises (docs/exercises.md)

| # | Asset | File | Status |
|---|-------|------|--------|
| ① | `top_selling_products` — join `raw_orders` + `raw_products`, top 5 produk berdasarkan total quantity terjual | `pipeline_products/main.py` | Selesai |
| ② | `orders_in_eur` — baca `orders`, `products` (ditulis `pipeline_products`) dan `exchange_rates` (ditulis `pipeline_fx`) langsung dari warehouse, konversi nilai order ke EUR | `pipeline_fx/main.py` | Selesai |
| ③ | `raw_orders_quantity_positive` — `@asset_check` pada `raw_orders`, gagal jika ada baris dengan `quantity <= 0` | `pipeline_products/main.py` | Selesai |

Semua diverifikasi lulus test (`pytest`): 6 test di `pipeline_products`, 5 test
di `pipeline_fx`.

## 3. Capstone: Track B — Cross-pipeline analytics

Saya memilih **Track B** karena tidak bergantung pada API eksternal baru
(lebih stabil) dan tidak memerlukan training model — cukup menggabungkan data
yang sudah ada di 3 pipeline lain menjadi satu laporan baru.

### Yang dibangun: `pipeline_reporting`

Container baru (gRPC port `4003`) yang membaca tabel dari warehouse dan
menghasilkan tabel laporan baru:

```
orders + products (pipeline_products)  --\
exchange_rates (pipeline_fx)             |--> high_value_orders_eur_report
order_value_predictions (pipeline_ml)  --/
```

**Asset:**
- `high_value_orders_eur_report` — join `orders` + `products` per `order_id`,
  hitung `total_eur` (dikonversi pakai kurs USD→EUR dari `pipeline_fx`), lalu
  gabung dengan agregat prediksi dari `pipeline_ml`
  (`predicted_high_value` = ada tidaknya baris prediksi high-value pada order
  tsb, `avg_probability` = rata-rata probabilitas model).
- `high_value_orders_eur_report_table` — menulis hasil ke warehouse.

**Quality gate:**
- `@asset_check` `report_has_no_duplicate_orders` — memastikan tidak ada
  `order_id` duplikat di laporan akhir (menjaga agregasi per-order tetap
  bersih).

**Struktur file:** `db.py`, `main.py`, `Dockerfile`, `requirements.txt`,
`.dockerignore`, `tests/` — mengikuti pola persis `pipeline_products` /
`pipeline_fx` / `pipeline_ml`.

### Wiring

- Ditambahkan sebagai code location baru di `workspace.yaml`
  (`pipeline_reporting`, port 4003).
- Ditambahkan sebagai service baru di `docker-compose.yml`, dan dimasukkan ke
  `depends_on` milik `dagster_webserver` dan `dagster_daemon`.
- `docker compose config --quiet` tervalidasi tanpa error.

### Testing

6 test lulus (`cd pipeline_reporting && pytest -v`):
- 3 unit test `db.py` (koneksi, tulis, baca)
- 2 unit test logika join/agregasi (`build_high_value_eur_report`), termasuk
  kasus gagal saat tidak ada kurs USD→EUR
- 1 test end-to-end (`dagster materialize`) yang memverifikasi pipeline
  berhasil dan asset check lulus

## 4. Self-check checklist (docs/capstone.md)

- [x] Pipeline baru muncul sebagai code location sendiri (`workspace.yaml`)
- [ ] "Materialize all" berhasil end-to-end di UI — *belum diverifikasi
      langsung karena Docker Desktop belum aktif saat pengerjaan; sudah
      tervalidasi via `docker compose config` dan seluruh unit test*
- [x] Ada `@asset_check` yang lulus (`report_has_no_duplicate_orders`)
- [x] `pytest` lulus untuk `pipeline_reporting/tests/`
- [x] Sudah di-wire ke `docker-compose.yml` dan `workspace.yaml`
- [ ] README fork diisi dengan `portfolio-readme-template.md` — *belum,
      karena repo belum di-fork ke akun GitHub pribadi*

## 5. Refleksi

Pola "satu container per pipeline, berbagi satu warehouse sebagai landing
zone" membuat pipeline baru bisa ditambahkan tanpa menyentuh pipeline yang
sudah ada sama sekali — `pipeline_reporting` hanya perlu tahu nama tabel yang
mau dibaca, tidak perlu dependency langsung ke container lain. Kalau ini versi
produksi, hal yang akan saya ubah: laporan sekarang truncate-and-load
(`if_exists="replace"`) sehingga histori tidak tersimpan — di produksi perlu
pola incremental/append dengan partisi tanggal, dan quality gate perlu lebih
dari sekadar cek duplikat (misalnya validasi rentang nilai `total_eur`).
