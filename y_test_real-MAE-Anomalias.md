# Cálculo de y_test_real, Naive MAE y Detección de Anomalías con Desviación Estándar

## Problema Identificado

En tu código tienes:
- `y_train`, `y_valid`, `y_test` están **NORMALIZADOS** (divididos por `scale`)
- Necesitas `y_test_real` para calcular el `naive_mae` en unidades reales (votos)
- Necesitas entender desviación estándar y varianza para detección de anomalías

---

## 1. Definición de Variables que Tienes

```python
# Estos son tus valores
scale = float(train.max())              # Ejemplo: 3,000,000 (máximo de votos observado)
y_train = train / scale                 # Normalizado, valores entre 0 y 1
y_valid = valid / scale                 # Normalizado
y_test = test / scale                   # Normalizado

# Recordatorio: train, valid, test son los datos ORIGINALES sin normalizar
train = series[:int(len(series)*0.8)]   # Datos reales de 0 a 115 grupos
valid = series[int(len(series)*0.8):int(len(series)*0.9)]  # Grupos 116 a 129
test = series[int(len(series)*0.9):]    # Grupos 130 a 143 (14 grupos en test)
```

---

## 2. Cálculo de y_test_real

**y_test_real es simplemente desnormalizar y_test:**

```python
# La forma correcta de calcular y_test_real
y_test_real = y_test * scale

# Ejemplo numérico:
# y_test normalizado: [0.5, 0.3, 0.4, 0.25, ...]
# scale: 3,000,000
# y_test_real: [1,500,000, 900,000, 1,200,000, 750,000, ...]
```

**Código completo en tu Jupyter:**

```python
# 1. Desnormalizar test set
y_test_real = y_test * scale

print(f"y_test (normalizado): {y_test[:5]}")
print(f"y_test_real (en votos): {y_test_real[:5]}")
print(f"Mínimo en test: {y_test_real.min():,.0f} votos")
print(f"Máximo en test: {y_test_real.max():,.0f} votos")
print(f"Promedio en test: {y_test_real.mean():,.0f} votos")
```

---

## 3. Cálculo del Naive MAE (Baseline)

El **baseline naive** es: "siempre predice la media del training set"

```python
# Paso 1: Calcular la media real de training (no normalizada)
# Opción A (correcta): usar los datos originales
mean_votes_train = train.mean()  # Media de train en votos reales

# Opción B (también correcta): desnormalizar la media normalizada
mean_votes_normalized = y_train.mean()  # Media de y_train (normalizado)
mean_votes_train_check = mean_votes_normalized * scale  # Debe dar lo mismo

print(f"Media de train en votos reales: {mean_votes_train:,.0f}")

# Paso 2: Calcular el error absoluto medio del naive predictor
# Fórmula: naive_mae = mean(|y_test_real - mean_votes_train|)
naive_mae = np.mean(np.abs(y_test_real - mean_votes_train))

# Paso 3: Comparar con tu LSTM MAE
print(f"\nNaive MAE (predecir siempre media): {naive_mae:,.0f} votos/5min")
print(f"LSTM MAE (tu modelo):               {valid_mae_lstm:,.0f} votos/5min")
print(f"\nMejora LSTM vs Baseline: {naive_mae / valid_mae_lstm:.1f}x mejor")
```

**Ejemplo numérico real:**

```
Supongamos:
- scale = 5,000,000
- mean_votes_train = 1,156,436
- y_test_real = [1,200,000, 950,000, 1,100,000, 1,050,000]

Naive predictor siempre predice: 1,156,436

Error de naive predictor:
  |1,200,000 - 1,156,436| = 43,564
  |950,000 - 1,156,436| = 206,436
  |1,100,000 - 1,156,436| = 56,436
  |1,050,000 - 1,156,436| = 106,436
  
naive_mae = (43,564 + 206,436 + 56,436 + 106,436) / 4 = 103,218

Tu LSTM MAE = 128  (MUCHO mejor!)
```

---

## 4. Desviación Estándar y Varianza

### Fórmulas

**Varianza (σ²):**

\[
\sigma^2 = \frac{1}{n} \sum_{i=1}^{n} (x_i - \mu)^2
\]

Donde:
- \(x_i\) = cada valor individual
- \(\mu\) = media
- \(n\) = número de valores

**Desviación Estándar (σ):**

\[
\sigma = \sqrt{\sigma^2}
\]

Simplemente la raíz cuadrada de la varianza.

### Interpretación

- **Desviación estándar pequeña**: Los valores están cerca de la media
- **Desviación estándar grande**: Los valores están dispersos lejos de la media

```python
# Ejemplo:
data1 = [100, 100, 100, 100, 100]  # Todos iguales
media1 = 100
std1 = 0  # CERO variabilidad

data2 = [50, 75, 100, 125, 150]  # Dispersos
media2 = 100
std2 = 40  # MÁS variabilidad
```

### Cálculo en Python

```python
import numpy as np

# Calcular desviación estándar de los errores de predicción
errors = np.abs(y_test_real - y_pred_lstm)  # Error de cada predicción

media_error = np.mean(errors)
std_error = np.std(errors)
varianza_error = np.var(errors)

print(f"Media de error:        {media_error:,.0f} votos")
print(f"Desviación estándar:   {std_error:,.0f} votos")
print(f"Varianza:              {varianza_error:,.0f} votos²")
```

---

## 5. Detección de Anomalías con Desviación Estándar

La idea clave: **Una anomalía es un punto donde el error de predicción es anormalmente grande**

### Método: Regla de 2-Sigma (95% de confianza)

```python
# Paso 1: Entrenar el modelo en datos NORMALES (train + valid)
# (Ya lo hiciste)

# Paso 2: Hacer predicciones en el SET DE TEST
y_pred_test_normalized = model_lstm.predict(test_ds, verbose=0)
y_pred_test = y_pred_test_normalized * scale  # Desnormalizar

# Paso 3: Calcular error en cada predicción
errors = np.abs(y_test_real - y_pred_test)

# Paso 4: Calcular media y desviación estándar de errores
media_error = np.mean(errors)
std_error = np.std(errors)

# Paso 5: Definir umbral (2-sigma)
# 95% de errores normales estarán dentro de media ± 2*sigma
threshold_normal = media_error + 2 * std_error
threshold_severe = media_error + 3 * std_error  # 99.7% confianza

print(f"Media de error:              {media_error:,.0f} votos")
print(f"Desv. Estándar de error:     {std_error:,.0f} votos")
print(f"Umbral anomalía (2σ):        {threshold_normal:,.0f} votos")
print(f"Umbral anomalía severa (3σ): {threshold_severe:,.0f} votos")

# Paso 6: Detectar anomalías
anomalias_2sigma = errors > threshold_normal
anomalias_3sigma = errors > threshold_severe

print(f"\nAnomalías detectadas (2σ): {np.sum(anomalias_2sigma)}")
print(f"Anomalías detectadas (3σ): {np.sum(anomalias_3sigma)}")

# Paso 7: Ver cuáles son
if np.any(anomalias_2sigma):
    print("\nGrupos con anomalías (2σ):")
    for idx in np.where(anomalias_2sigma)[0]:
        print(f"  Grupo {idx}: error={errors[idx]:,.0f}, "
              f"real={y_test_real[idx]:,.0f}, "
              f"predicho={y_pred_test[idx]:,.0f}")
```

---

## 6. Interpretación de Desviación Estándar en Contexto de Anomalías

### Regla de Sigma (Distribución Normal)

| Rango | Porcentaje | Significado |
|-------|-----------|-------------|
| μ ± 1σ | 68.2% | Normal, casi todos caen aquí |
| μ ± 2σ | 95.4% | Bastante normal, algunos raros |
| μ ± 3σ | 99.7% | Muy raro, probablemente anomalía |
| > 3σ | 0.3% | Definida anomalía (1 en 300) |

### Aplicado a Tu Problema

Supongamos:
```
media_error = 500 votos
std_error = 200 votos

1σ rango: [300, 700] → Normal
2σ rango: [100, 900] → Bastante normal
3σ rango: [-100, 1100] → Muy raro si error > 1,100

Si un error es 2,000 votos → DEFINIDA ANOMALÍA
```

---

## 7. Código Completo Integrado

```python
import numpy as np
import matplotlib.pyplot as plt

# ========== PASO 1: Desnormalizar test set ==========
y_test_real = y_test * scale

# ========== PASO 2: Calcular Naive MAE ==========
mean_votes_train = train.mean()
naive_mae = np.mean(np.abs(y_test_real - mean_votes_train))

print("=" * 70)
print("COMPARACIÓN LSTM vs BASELINE")
print("=" * 70)
print(f"Naive MAE (predecir media):  {naive_mae:,.0f} votos/5min")
print(f"LSTM MAE (tu modelo):        {valid_mae_lstm:,.0f} votos/5min")
print(f"Mejora LSTM vs Baseline:     {naive_mae/valid_mae_lstm:.1f}x MEJOR")

# ========== PASO 3: Hacer predicciones en test ==========
y_pred_test_norm = model_lstm.predict(test_ds, verbose=0)
y_pred_test = y_pred_test_norm * scale

# ========== PASO 4: Calcular errores ==========
errors = np.abs(y_test_real - y_pred_test)

# ========== PASO 5: Estadísticas de errores ==========
media_error = np.mean(errors)
std_error = np.std(errors)
varianza_error = np.var(errors)
min_error = np.min(errors)
max_error = np.max(errors)

print("\n" + "=" * 70)
print("ESTADÍSTICAS DE ERRORES DE PREDICCIÓN")
print("=" * 70)
print(f"Media de error:              {media_error:,.0f} votos")
print(f"Desviación estándar (σ):     {std_error:,.0f} votos")
print(f"Varianza (σ²):               {varianza_error:,.0f} votos²")
print(f"Error mínimo:                {min_error:,.0f} votos")
print(f"Error máximo:                {max_error:,.0f} votos")

# ========== PASO 6: Umbrales de anomalía ==========
threshold_1sigma = media_error + std_error
threshold_2sigma = media_error + 2 * std_error
threshold_3sigma = media_error + 3 * std_error

print("\n" + "=" * 70)
print("UMBRALES DE ANOMALÍA")
print("=" * 70)
print(f"Rango normal (1σ):   0 - {threshold_1sigma:,.0f} votos (68% de casos)")
print(f"Rango raro (2σ):     0 - {threshold_2sigma:,.0f} votos (95% de casos)")
print(f"Rango anómalo (3σ):  > {threshold_3sigma:,.0f} votos (anomalía probable)")

# ========== PASO 7: Detectar anomalías ==========
anomalias = errors > threshold_2sigma

print(f"\n" + "=" * 70)
print(f"ANOMALÍAS DETECTADAS (2σ umbral)")
print(f"=" * 70)
print(f"Total de predicciones: {len(errors)}")
print(f"Anomalías encontradas: {np.sum(anomalias)}")

if np.any(anomalias):
    print("\nDetalles de anomalías:")
    for idx in np.where(anomalias)[0]:
        grupo = 130 + idx  # Adjust to actual group number
        print(f"\n  Grupo #{grupo} (índice {idx}):")
        print(f"    Votos reales:        {y_test_real[idx]:,.0f}")
        print(f"    Votos predichos:     {y_pred_test[idx]:,.0f}")
        print(f"    Error de predicción: {errors[idx]:,.0f} (> {threshold_2sigma:,.0f})")
        sigma_deviations = (errors[idx] - media_error) / std_error
        print(f"    Desviaciones sigma:  {sigma_deviations:.2f}σ (anómalo si > 2)")

# ========== PASO 8: Visualización ==========
plt.figure(figsize=(14, 5))

# Gráfico 1: Distribución de errores
plt.subplot(1, 2, 1)
plt.hist(errors, bins=10, alpha=0.7, color='blue', edgecolor='black')
plt.axvline(media_error, color='green', linestyle='--', linewidth=2, label='Media')
plt.axvline(threshold_2sigma, color='red', linestyle='--', linewidth=2, label='Umbral 2σ')
plt.xlabel('Error (votos)')
plt.ylabel('Frecuencia')
plt.title('Distribución de Errores de Predicción')
plt.legend()
plt.grid(True, alpha=0.3)

# Gráfico 2: Predicciones vs Reales
plt.subplot(1, 2, 2)
indices = np.arange(len(y_test_real))
plt.plot(indices, y_test_real, 'o-', label='Real', linewidth=2, markersize=6)
plt.plot(indices, y_pred_test, 's--', label='Predicción LSTM', linewidth=2, markersize=6)
plt.scatter(indices[anomalias], y_test_real[anomalias], 
           color='red', s=200, marker='X', label='Anomalías', zorder=5)
plt.xlabel('Índice de Grupo de Test')
plt.ylabel('Votos')
plt.title('Predicciones LSTM vs Valores Reales')
plt.legend()
plt.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()
```

---

## 8. Glosario de Términos

**Anomalía**: Un punto de datos que se desvía significativamente del patrón normal, indicando posible fraude, error o evento inusual en votaciones.

**Baseline (Naive)**: Predictor trivial que siempre predice el mismo valor (la media). Sirve como punto de comparación para validar si tu modelo es útil.

**Desviación Estándar (σ)**: Medida de dispersión. Indica cuánto varían los datos alrededor de la media. Unidades: mismas que los datos.

**Desnormalizar**: Convertir datos normalizados (0-1) de vuelta a unidades originales multiplicando por la escala.

**Error Absoluto**: Diferencia entre valor real y predicción, sin importar el signo: |real - predicho|.

**MAE (Mean Absolute Error)**: Promedio de los errores absolutos. Métrica de evaluación más interpretable que MSE.

**Media (μ)**: Promedio de todos los valores.

**Normalizar**: Escalar datos a un rango común (0-1) dividiéndolos por un valor máximo.

**Regla de Sigma (σ)**: En una distribución normal, 68% de datos caen dentro de ±1σ, 95% dentro de ±2σ, 99.7% dentro de ±3σ.

**Threshold (Umbral)**: Valor límite por encima del cual se considera un caso como anómalo.

**Varianza (σ²)**: Promedio de las desviaciones cuadradas de la media. Unidades: datos².

**y_test_real**: Valores reales de test sin normalizar (en votos originales, no en rango 0-1).

---

## 9. Resumen de Fórmulas

```
y_test_real = y_test * scale

mean_votes_train = train.mean()

naive_mae = mean(|y_test_real - mean_votes_train|)

media_error = mean(errors)

std_error = sqrt(sum((error_i - media_error)²) / n)

threshold_2sigma = media_error + 2 * std_error

Es anomalía si: error_i > threshold_2sigma
```

---

**Conclusión**: Ahora tienes todas las herramientas para:
1. Desnormalizar datos correctamente
2. Calcular baseline y compararlo con tu LSTM
3. Entender desviación estándar en contexto de anomalías
4. Detectar automáticamente puntos anómalos en votaciones
